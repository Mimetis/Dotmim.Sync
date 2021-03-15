using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class BaseOrchestrator
    {
        // Collection of Interceptors
        private Interceptors interceptors = new Interceptors();
        internal SyncContext syncContext;

        //// Internal table builder cache
        //private static ConcurrentDictionary<string, Lazy<DbTableBuilder>> tableBuilders
        //    = new ConcurrentDictionary<string, Lazy<DbTableBuilder>>();

        //// Internal scope builder cache
        //private static ConcurrentDictionary<string, Lazy<DbScopeBuilder>> scopeBuilders
        //    = new ConcurrentDictionary<string, Lazy<DbScopeBuilder>>();

        // Internal sync adapter cache
        //private static ConcurrentDictionary<string, Lazy<DbSyncAdapter>> syncAdapters
        //    = new ConcurrentDictionary<string, Lazy<DbSyncAdapter>>();

        /// <summary>
        /// Gets or Sets orchestrator side
        /// </summary>
        public abstract SyncSide Side { get; }

        /// <summary>
        /// Gets or Sets the provider used by this local orchestrator
        /// </summary>
        public virtual CoreProvider Provider { get; set; }

        /// <summary>
        /// Gets the options used by this local orchestrator
        /// </summary>
        public virtual SyncOptions Options { get; internal set; }

        /// <summary>
        /// Gets the Setup used by this local orchestrator
        /// </summary>
        public virtual SyncSetup Setup { get; set; }

        /// <summary>
        /// Gets the scope name used by this local orchestrator
        /// </summary>
        public virtual string ScopeName { get; internal protected set; }

        /// <summary>
        /// Gets or Sets the start time for this orchestrator
        /// </summary>
        public virtual DateTime? StartTime { get; set; }

        /// <summary>
        /// Gets or Sets the end time for this orchestrator
        /// </summary>
        public virtual DateTime? CompleteTime { get; set; }

        /// <summary>
        /// Gets or Sets the logger used by this orchestrator
        /// </summary>
        public virtual ILogger Logger { get; set; }


        /// <summary>
        /// Create a local orchestrator, used to orchestrates the whole sync on the client side
        /// </summary>
        public BaseOrchestrator(CoreProvider provider, SyncOptions options, SyncSetup setup, string scopeName = SyncOptions.DefaultScopeName)
        {
            this.ScopeName = scopeName ?? throw new ArgumentNullException(nameof(scopeName));
            this.Provider = provider ?? throw new ArgumentNullException(nameof(provider));
            this.Options = options ?? throw new ArgumentNullException(nameof(options));
            this.Setup = setup ?? throw new ArgumentNullException(nameof(setup));

            this.Provider.Orchestrator = this;
            this.Logger = options.Logger;
        }

        /// <summary>
        /// Set an interceptor to get info on the current sync process
        /// </summary>
        [DebuggerStepThrough]
        internal void On<T>(Action<T> interceptorAction) where T : ProgressArgs =>
            this.interceptors.GetInterceptor<T>().Set(interceptorAction);

        /// <summary>
        /// Set an interceptor to get info on the current sync process
        /// </summary>
        [DebuggerStepThrough]
        internal void On<T>(Func<T, Task> interceptorAction) where T : ProgressArgs =>
            this.interceptors.GetInterceptor<T>().Set(interceptorAction);

        /// <summary>
        /// Set a collection of interceptors
        /// </summary>
        [DebuggerStepThrough]
        internal void On(Interceptors interceptors) => this.interceptors = interceptors;

        /// <summary>
        /// Returns the Task associated with given type of BaseArgs 
        /// Because we are not doing anything else than just returning a task, no need to use async / await. Just return the Task itself
        /// </summary>
        internal async Task InterceptAsync<T>(T args, CancellationToken cancellationToken) where T : ProgressArgs
        {
            if (this.interceptors == null)
                return;

            var interceptor = this.interceptors.GetInterceptor<T>();

            // Check logger, because we make some reflection here
            if (this.Logger.IsEnabled(LogLevel.Debug))
            {
                //for example, getting DatabaseChangesSelectingArgs and transform to DatabaseChangesSelecting
                var argsTypeName = args.GetType().Name.Replace("Args", "");

                this.Logger.LogDebug(new EventId(args.EventId, argsTypeName), args);
            }

            await interceptor.RunAsync(args, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Affect an interceptor
        /// </summary>
        [DebuggerStepThrough]
        internal void SetInterceptor<T>(Action<T> action) where T : ProgressArgs => this.On(action);

        /// <summary>
        /// Affect an interceptor
        /// </summary>
        [DebuggerStepThrough]
        internal void SetInterceptor<T>(Func<T, Task> action) where T : ProgressArgs => this.On(action);

        /// <summary>
        /// Gets a boolean returning true if an interceptor of type T, exists
        /// </summary>
        [DebuggerStepThrough]
        internal bool ContainsInterceptor<T>() where T : ProgressArgs => this.interceptors.Contains<T>();

        /// <summary>
        /// Try to report progress
        /// </summary>
        internal void ReportProgress(SyncContext context, IProgress<ProgressArgs> progress, ProgressArgs args, DbConnection connection = null, DbTransaction transaction = null)
        {
            // Check logger, because we make some reflection here
            if (this.Logger.IsEnabled(LogLevel.Information))
            {
                var argsTypeName = args.GetType().Name.Replace("Args", ""); ;
                if (this.Logger.IsEnabled(LogLevel.Debug))
                    this.Logger.LogDebug(new EventId(args.EventId, argsTypeName), args.Context);
                else
                    this.Logger.LogInformation(new EventId(args.EventId, argsTypeName), args);
            }

            if (progress == null)
                return;

            if (connection == null && args.Connection != null)
                connection = args.Connection;

            if (transaction == null && args.Transaction != null)
                transaction = args.Transaction;

            if (args.Connection == null || args.Connection != connection)
                args.Connection = connection;

            if (args.Transaction== null || args.Transaction != transaction)
                args.Transaction = transaction;

            progress.Report(args);
        }

        /// <summary>
        /// Open a connection
        /// </summary>
        [DebuggerStepThrough]
        internal async Task OpenConnectionAsync(DbConnection connection, CancellationToken cancellationToken)
        {
            // Make an interceptor when retrying to connect
            var onRetry = new Func<Exception, int, TimeSpan, object, Task>((ex, cpt, ts, arg) =>
                this.InterceptAsync(new ReConnectArgs(this.GetContext(), connection, ex, cpt, ts), cancellationToken));

            // Defining my retry policy
            var policy = SyncPolicy.WaitAndRetry(
                                3,
                                retryAttempt => TimeSpan.FromMilliseconds(500 * retryAttempt),
                                (ex, arg) => this.Provider.ShouldRetryOn(ex),
                                onRetry);

            // Execute my OpenAsync in my policy context
            await policy.ExecuteAsync(ct => connection.OpenAsync(ct), cancellationToken);

            // Let provider knows a connection is opened
            this.Provider.OnConnectionOpened(connection);

            await this.InterceptAsync(new ConnectionOpenedArgs(this.GetContext(), connection), cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Close a connection
        /// </summary>
        [DebuggerStepThrough]
        internal async Task CloseConnectionAsync(DbConnection connection, CancellationToken cancellationToken)
        {
            if (connection != null && connection.State == ConnectionState.Closed)
                return;

            if (connection != null && connection.State == ConnectionState.Open)
            {
                connection.Close();
                connection.Dispose();
            }

            if (!cancellationToken.IsCancellationRequested)
                await this.InterceptAsync(new ConnectionClosedArgs(this.GetContext(), connection), cancellationToken).ConfigureAwait(false);

            // Let provider knows a connection is closed
            this.Provider.OnConnectionClosed(connection);
        }

        /// <summary>
        /// Encapsulates an error in a SyncException, let provider enrich the error if needed, then throw again
        /// </summary>
        [DebuggerStepThrough]
        internal void RaiseError(Exception exception)
        {
            var syncException = new SyncException(exception, this.GetContext().SyncStage);

            // try to let the provider enrich the exception
            this.Provider.EnsureSyncException(syncException);
            syncException.Side = this.Side;

            this.Logger.LogError(SyncEventsId.Exception, syncException, syncException.Message);

            throw syncException;
        }

        /// <summary>
        /// Get the provider sync adapter
        /// </summary>
        public DbSyncAdapter GetSyncAdapter(SyncTable tableDescription, SyncSetup setup)
        {
            //var p = this.Provider.GetParsers(tableDescription, setup);

            //var s = JsonConvert.SerializeObject(setup);
            //var data = Encoding.UTF8.GetBytes(s);
            //var hash = HashAlgorithm.SHA256.Create(data);
            //var hashString = Convert.ToBase64String(hash);

            //// Create the key
            //var commandKey = $"{p.tableName.ToString()}-{p.trackingName.ToString()}-{hashString}-{this.Provider.ConnectionString}";

            //// Get a lazy command instance
            //var lazySyncAdapter = syncAdapters.GetOrAdd(commandKey,
            //    k => new Lazy<DbSyncAdapter>(() => this.Provider.GetSyncAdapter(tableDescription, setup)));

            //// Get the concrete instance
            //var syncAdapter = lazySyncAdapter.Value;

            //return syncAdapter;

            return this.Provider.GetSyncAdapter(tableDescription, setup);
        }

        /// <summary>
        /// Get the provider table builder
        /// </summary>
        public DbTableBuilder GetTableBuilder(SyncTable tableDescription, SyncSetup setup)
        {
            //var p = this.Provider.GetParsers(tableDescription, setup);

            //var s = JsonConvert.SerializeObject(setup);
            //var data = Encoding.UTF8.GetBytes(s);
            //var hash = HashAlgorithm.SHA256.Create(data);
            //var hashString = Convert.ToBase64String(hash);

            //// Create the key
            //var commandKey = $"{p.tableName.ToString()}-{p.trackingName.ToString()}-{hashString}-{this.Provider.ConnectionString}";

            //// Get a lazy command instance
            //var lazyTableBuilder = tableBuilders.GetOrAdd(commandKey,
            //    k => new Lazy<DbTableBuilder>(() => this.Provider.GetTableBuilder(tableDescription, setup)));

            //// Get the concrete instance
            //var tableBuilder = lazyTableBuilder.Value;

            //return tableBuilder;

            return this.Provider.GetTableBuilder(tableDescription, setup);
        }


        /// <summary>
        /// Get a provider scope builder by scope table name
        /// </summary>
        public DbScopeBuilder GetScopeBuilder(string scopeInfoTableName)
        {
            //// Create the key
            //var commandKey = $"{scopeInfoTableName}-{this.Provider.ConnectionString}";

            //// Get a lazy command instance
            //var lazyScopeBuilder = scopeBuilders.GetOrAdd(commandKey,
            //    k => new Lazy<DbScopeBuilder>(() => this.Provider.GetScopeBuilder(scopeInfoTableName)));

            //// Get the concrete instance
            //var scopeBuilder = lazyScopeBuilder.Value;

            //return scopeBuilder;

            return this.Provider.GetScopeBuilder(scopeInfoTableName);
        }
        /// <summary>
        /// Sets the current context
        /// </summary>
        internal virtual void SetContext(SyncContext context) => this.syncContext = context;

        /// <summary>
        /// Gets the current context
        /// </summary>
        [DebuggerStepThrough]
        public virtual SyncContext GetContext()
        {
            if (this.syncContext != null)
                return this.syncContext;

            this.syncContext = new SyncContext(Guid.NewGuid(), this.ScopeName); ;

            return this.syncContext;
        }


        /// <summary>
        /// Check if the orchestrator database is outdated
        /// </summary>
        /// <param name="timeStampStart">Timestamp start. Used to limit the delete metadatas rows from now to this timestamp</param>
        public virtual async Task<bool> IsOutDated(ScopeInfo clientScopeInfo, ServerScopeInfo serverScopeInfo, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            bool isOutdated = false;

            // Get context or create a new one
            var ctx = this.GetContext();

            // if we have a new client, obviously the last server sync is < to server stored last clean up (means OutDated !)
            // so far we return directly false
            if (clientScopeInfo.IsNewScope)
                return false;

            // Check if the provider is not outdated
            // We can have negative value where we want to compare anyway
            if (clientScopeInfo.LastServerSyncTimestamp != 0 || serverScopeInfo.LastCleanupTimestamp != 0)
                isOutdated = clientScopeInfo.LastServerSyncTimestamp < serverScopeInfo.LastCleanupTimestamp;

            // Get a chance to make the sync even if it's outdated
            if (isOutdated)
            {
                var outdatedArgs = new OutdatedArgs(ctx, clientScopeInfo, serverScopeInfo);

                // Interceptor
                await this.InterceptAsync(outdatedArgs, cancellationToken).ConfigureAwait(false);

                if (outdatedArgs.Action != OutdatedAction.Rollback)
                    ctx.SyncType = outdatedArgs.Action == OutdatedAction.Reinitialize ? SyncType.Reinitialize : SyncType.ReinitializeWithUpload;

                if (outdatedArgs.Action == OutdatedAction.Rollback)
                    throw new OutOfDateException(clientScopeInfo.LastServerSyncTimestamp, serverScopeInfo.LastCleanupTimestamp);
            }

            return isOutdated;
        }

        /// <summary>
        /// Get hello from database
        /// </summary>
        public virtual async Task<(SyncContext SyncContext, string DatabaseName, string Version)> GetHelloAsync(SyncContext context, DbConnection connection, DbTransaction transaction,
                               CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // get database builder
            var databaseBuilder = this.Provider.GetDatabaseBuilder();

            var hello = await databaseBuilder.GetHelloAsync(connection, transaction);

            return (context, hello.DatabaseName, hello.Version);
        }



        public async Task<T> RunInTransactionAsync2<T>(SyncStage stage = SyncStage.None, Func<SyncContext, DbConnection, DbTransaction, Task<T>> actionTask = null,
              DbConnection connection = null, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();

            T result = default;

            using (var c = this.Provider.CreateConnection())
            {
                try
                {
                    await c.OpenAsync();

                    using (var t = c.BeginTransaction())
                    {
                        if (actionTask != null)
                            result = await actionTask(ctx, c, t);

                        t.Commit();
                    }
                    c.Close();
                    c.Dispose();

                    return result;
                }
                catch (Exception ex)
                {
                    RaiseError(ex);
                }
            }

            return default;
        }

        /// <summary>
        /// Run an actin inside a connection / transaction
        /// </summary>
        public async Task<T> RunInTransactionAsync<T>(SyncStage stage = SyncStage.None, Func<SyncContext, DbConnection, DbTransaction, Task<T>> actionTask = null,
              DbConnection connection = null, DbTransaction transaction = null, CancellationToken cancellationToken = default)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();

            T result = default;

            if (connection == null)
                connection = this.Provider.CreateConnection();

            var alreadyOpened = connection.State == ConnectionState.Open;
            var alreadyInTransaction = transaction != null && transaction.Connection == connection;

            try
            {
                if (stage != SyncStage.None)
                    ctx.SyncStage = stage;

                // Open connection
                if (!alreadyOpened)
                    await this.OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                // Create a transaction
                if (!alreadyInTransaction)
                {
                    transaction = connection.BeginTransaction();
                    await this.InterceptAsync(new TransactionOpenedArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);
                }

                if (actionTask != null)
                    result = await actionTask(ctx, connection, transaction);

                if (!alreadyInTransaction)
                {
                    await this.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);
                    transaction.Commit();
                    transaction.Dispose();
                }

                if (!alreadyOpened)
                    await this.CloseConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                return result;
            }
            catch (Exception ex)
            {
                RaiseError(ex);
            }
            finally
            {
                if (!alreadyOpened)
                    await this.CloseConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
            }

            return default;
        }

        /// <summary>
        /// Get a snapshot root directory name and folder directory name
        /// </summary>
        public virtual Task<(string DirectoryRoot, string DirectoryName)> GetSnapshotDirectoryAsync(SyncParameters syncParameters = null, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            // Get context or create a new one
            var ctx = this.GetContext();

            // check parameters
            // If context has no parameters specified, and user specifies a parameter collection we switch them
            if ((ctx.Parameters == null || ctx.Parameters.Count <= 0) && syncParameters != null && syncParameters.Count > 0)
                ctx.Parameters = syncParameters;

            return this.InternalGetSnapshotDirectoryAsync(ctx, cancellationToken, progress);
        }

        /// <summary>
        /// Internal routine to clean tmp folders. MUST be compare also with Options.CleanFolder
        /// </summary>
        internal virtual async Task<bool> InternalCanCleanFolderAsync(SyncContext context, BatchInfo batchInfo,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            // if in memory, the batch file is not actually serialized on disk
            if (batchInfo.InMemory)
                return false;

            var batchInfoDirectoryFullPath = new DirectoryInfo(batchInfo.GetDirectoryFullPath());

            var (snapshotRootDirectory, snapshotNameDirectory) = await this.GetSnapshotDirectoryAsync();

            // if we don't have any snapshot configuration, we are sure that the current batchinfo is actually stored into a temp folder
            if (string.IsNullOrEmpty(snapshotRootDirectory))
                return true;

            var snapInfo = Path.Combine(snapshotRootDirectory, snapshotNameDirectory);
            var snapshotDirectoryFullPath = new DirectoryInfo(snapInfo);

            // check if the batch dir IS NOT the snapshot directory
            var canCleanFolder = batchInfoDirectoryFullPath.FullName != snapshotDirectoryFullPath.FullName;

            return canCleanFolder;
        }

        /// <summary>
        /// Internal routine to get the snapshot root directory and batch directory name
        /// </summary>
        internal virtual Task<(string DirectoryRoot, string DirectoryName)> InternalGetSnapshotDirectoryAsync(SyncContext context,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            if (string.IsNullOrEmpty(this.Options.SnapshotsDirectory))
                return Task.FromResult<(string, string)>((default, default));

            // cleansing scope name
            var directoryScopeName = new string(context.ScopeName.Where(char.IsLetterOrDigit).ToArray());

            var directoryFullPath = Path.Combine(this.Options.SnapshotsDirectory, directoryScopeName);

            var sb = new StringBuilder();
            var underscore = "";

            if (context.Parameters != null)
            {
                foreach (var p in context.Parameters.OrderBy(p => p.Name))
                {
                    var cleanValue = new string(p.Value.ToString().Where(char.IsLetterOrDigit).ToArray());
                    var cleanName = new string(p.Name.Where(char.IsLetterOrDigit).ToArray());

                    sb.Append($"{underscore}{cleanName}_{cleanValue}");
                    underscore = "_";
                }
            }

            var directoryName = sb.ToString();
            directoryName = string.IsNullOrEmpty(directoryName) ? "ALL" : directoryName;

            return Task.FromResult((directoryFullPath, directoryName));

        }
    }
}
