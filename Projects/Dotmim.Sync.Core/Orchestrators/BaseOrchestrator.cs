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
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class BaseOrchestrator
    {
        // Collection of Interceptors
        internal Interceptors interceptors = new();

        /// <summary>
        /// Gets or Sets the provider used by this local orchestrator
        /// </summary>
        public virtual CoreProvider Provider { get; set; }

        /// <summary>
        /// Gets the options used by this local orchestrator
        /// </summary>
        public virtual SyncOptions Options { get; internal set; }

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
        public BaseOrchestrator(CoreProvider provider, SyncOptions options)
        {
            this.Options = options ?? throw GetSyncError(null, new ArgumentNullException(nameof(options)));

            if (provider != null)
            {
                this.Provider = provider;
                this.Provider.Orchestrator = this;
            }

            this.Logger = options.Logger;
        }

        /// <summary>
        /// Add an interceptor of T
        /// </summary>
        internal virtual Guid AddInterceptor<T>(Action<T> action) where T : ProgressArgs => this.interceptors.Add(action);

        /// <summary>
        /// Add an async interceptor of T
        /// </summary>
        internal virtual Guid AddInterceptor<T>(Func<T, Task> action) where T : ProgressArgs => this.interceptors.Add(action);

        /// <summary>
        /// Remove all interceptors based on type of ProgressArgs
        /// </summary>
        public void ClearInterceptors<T>() where T : ProgressArgs => this.interceptors.Clear<T>();

        /// <summary>
        /// Remove all interceptors 
        /// </summary>
        public void ClearInterceptors() => this.interceptors.Clear();

        /// <summary>
        /// Remove interceptor based on Id
        /// </summary>
        public void ClearInterceptors(Guid id) => this.interceptors.Clear(id);

        /// <summary>
        /// Returns a boolean value indicating if we have any interceptors for the current type T
        /// </summary>
        public bool HasInterceptors<T>() where T : ProgressArgs
        {
            if (this.interceptors == null)
                return false;

            var interceptors = this.interceptors.GetInterceptors<T>();

            return interceptors.Any();

        }

        /// <summary>
        /// Try to proc a On[Method]
        /// </summary>
        internal async Task<T> InterceptAsync<T>(T args, IProgress<ProgressArgs> progress = default, CancellationToken cancellationToken = default) where T : ProgressArgs
        {
            if (this.interceptors == null)
                return args;

            var interceptors = this.interceptors.GetInterceptors<T>();

            // Check logger, because we make some reflection here
            if (this.Logger.IsEnabled(LogLevel.Debug))
            {
                //for example, getting DatabaseChangesSelectingArgs and transform to DatabaseChangesSelecting
                var argsTypeName = args.GetType().Name.Replace("Args", "");

                this.Logger.LogDebug(new EventId(args.EventId, argsTypeName), args);
            }

            foreach (var interceptor in interceptors)
                await interceptor.RunAsync(args, cancellationToken).ConfigureAwait(false);

            if (progress != default)
                this.ReportProgress(args.Context, progress, args, args.Connection, args.Transaction);

            return args;
        }

        /// <summary>
        /// Try to report progress
        /// </summary>
        internal void ReportProgress(SyncContext context, IProgress<ProgressArgs> progress, ProgressArgs args, DbConnection connection = null, DbTransaction transaction = null)
        {
            // Check logger, because we make some reflection here
            if (this.Logger.IsEnabled(LogLevel.Information))
            {
                var argsTypeName = args.GetType().Name.Replace("Args", "");
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

            if (args.Transaction == null || args.Transaction != transaction)
                args.Transaction = transaction;

            if (this.Options.ProgressLevel <= args.ProgressLevel)
                progress.Report(args);
        }

        /// <summary>
        /// Open a connection
        /// </summary>
        [DebuggerStepThrough]
        internal virtual async Task OpenConnectionAsync(SyncContext context, DbConnection connection, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            if (this.Provider == null)
                return;

            // Make an interceptor when retrying to connect
            var onRetry = new Func<Exception, int, TimeSpan, object, Task>((ex, cpt, ts, arg) =>
                this.InterceptAsync(new ReConnectArgs(context, connection, ex, cpt, ts), progress, cancellationToken));

            // Defining my retry policy
            var policy = SyncPolicy.WaitAndRetry(
                                3,
                                retryAttempt => TimeSpan.FromMilliseconds(500 * retryAttempt),
                                (ex, arg) => this.Provider.ShouldRetryOn(ex),
                                onRetry);

            // Execute my OpenAsync in my policy context
            await policy.ExecuteAsync(ct => connection.OpenAsync(ct), cancellationToken);

            // Let provider knows a connection is opened
            this.Provider.onConnectionOpened(connection);

            await this.InterceptAsync(new ConnectionOpenedArgs(context, connection), progress, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Close a connection
        /// </summary>
        internal virtual async Task CloseConnectionAsync(SyncContext context, DbConnection connection, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            if (this.Provider == null)
                return;

            if (connection != null && connection.State == ConnectionState.Closed)
                return;

            bool isClosedHere = false;

            if (connection != null && connection.State == ConnectionState.Open)
            {
                connection.Close();
                isClosedHere = true;
            }

            if (!cancellationToken.IsCancellationRequested)
                await this.InterceptAsync(new ConnectionClosedArgs(context, connection), progress, cancellationToken).ConfigureAwait(false);

            // Let provider knows a connection is closed
            this.Provider.onConnectionClosed(connection);

            if (isClosedHere && connection != null)
                connection.Dispose();
        }


        //[DebuggerStepThrough]
        internal SyncException GetSyncError(SyncContext context, Exception innerException, string message = default, [CallerMemberName] string methodName = null)
        {
            // First we log the error before adding a new layer
            if (this.Logger != null)
                this.Logger.LogError(SyncEventsId.Exception, innerException, innerException.Message);

            var strSyncStage = context == null ? SyncStage.None : context.SyncStage;
            var strScopeName = context == null ? null : $"[{context.ScopeName}].";
            var strMethodName = string.IsNullOrEmpty(methodName) ? "" : $"[{methodName}].";
            var strMessage = string.IsNullOrEmpty(message) ? "" : message;

            var strDataSource = innerException is SyncException se ? se.DataSource : "";
            strDataSource = string.IsNullOrEmpty(strDataSource) ? "" : $"[{strDataSource}].";

            var strInitialCatalog = innerException is SyncException se2 ? se2.InitialCatalog : "";
            strInitialCatalog = string.IsNullOrEmpty(strInitialCatalog) ? "" : $"[{strInitialCatalog}].";

            message = $"{strDataSource}{strInitialCatalog}{strScopeName}{strMethodName}{strMessage}";

            var baseMessage = innerException.Message;

            if (innerException is SyncException se3)
            {
                if (!string.IsNullOrEmpty(se3.BaseMessage))
                    baseMessage = se3.BaseMessage;
            }

            message += $":{baseMessage}";

            var syncException = new SyncException(innerException, message, strSyncStage)
            {
                BaseMessage = baseMessage
            };

            // try to let the provider enrich the exception
            if (this.Provider != null)
                this.Provider.EnsureSyncException(syncException);

            return syncException;
        }

        /// <summary>
        /// Get the provider sync adapter
        /// </summary>
        public virtual DbSyncAdapter GetSyncAdapter(string scopeName, SyncTable tableDescription, SyncSetup setup = default)
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
            if (this.Provider == null)
                return null;

            var (tableName, trackingTableName) = this.Provider.GetParsers(tableDescription, setup);
            return this.Provider.GetSyncAdapter(tableDescription, tableName, trackingTableName, setup, scopeName);
        }

        /// <summary>
        /// Get the provider table builder
        /// </summary>
        public DbTableBuilder GetTableBuilder(SyncTable tableDescription, ScopeInfo scopeInfo)
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

            if (this.Provider == null)
                return null;

            var (tableName, trackingTableName) = this.Provider.GetParsers(tableDescription, scopeInfo.Setup);
            return this.Provider.GetTableBuilder(tableDescription, tableName, trackingTableName, scopeInfo.Setup, scopeInfo.Name);
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
            if (this.Provider == null)
                return null;

            return this.Provider.GetScopeBuilder(scopeInfoTableName);
        }

        /// <summary>
        /// Check if the orchestrator database is outdated
        /// </summary>
        internal virtual async Task<(SyncContext, bool)> InternalIsOutDatedAsync(SyncContext context, ScopeInfoClient cScopeInfoClient, ScopeInfo sScopeInfo, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            bool isOutdated = false;

            // if we have a new client, obviously the last server sync is < to server stored last clean up (means OutDated !)
            // so far we return directly false
            if (cScopeInfoClient.IsNewScope)
                return (context, false);

            if (cScopeInfoClient.LastServerSyncTimestamp.HasValue && sScopeInfo.LastCleanupTimestamp.HasValue)
                isOutdated = cScopeInfoClient.LastServerSyncTimestamp < sScopeInfo.LastCleanupTimestamp;

            // Get a chance to make the sync even if it's outdated
            if (isOutdated)
            {
                var outdatedArgs = new OutdatedArgs(context, cScopeInfoClient, sScopeInfo);

                // Interceptor
                await this.InterceptAsync(outdatedArgs, progress, cancellationToken).ConfigureAwait(false);

                if (outdatedArgs.Action != OutdatedAction.Rollback)
                {
                    context.SyncType = outdatedArgs.Action == OutdatedAction.Reinitialize ? SyncType.Reinitialize : SyncType.ReinitializeWithUpload;
                }

                if (outdatedArgs.Action == OutdatedAction.Rollback)
                    throw new OutOfDateException(cScopeInfoClient.LastServerSyncTimestamp, sScopeInfo.LastCleanupTimestamp);
            }

            return (context, isOutdated);
        }

        /// <summary>
        /// Check if a database exists, regarding the provider you are using. Returns database name and database version.
        /// </summary>
        public virtual Task<(SyncContext context, string DatabaseName, string Version)> GetHelloAsync() => GetHelloAsync(SyncOptions.DefaultScopeName);

        /// <summary>
        /// Check if a database exists, regarding the provider you are using. Returns database name and database version.
        /// </summary>
        public virtual Task<(SyncContext context, string DatabaseName, string Version)> GetHelloAsync(string scopeName)
            => InternalGetHelloAsync(new SyncContext(Guid.NewGuid(), scopeName), default, default, default, default);

        /// <summary>
        /// Get hello from database
        /// </summary>
        internal virtual async Task<(SyncContext context, string DatabaseName, string Version)> InternalGetHelloAsync(SyncContext context,
            DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = default)
        {
            if (this.Provider == null)
                return (context, default, default);

            try
            {
                // TODO : get all scopes for Hello all of them
                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                var databaseBuilder = this.Provider.GetDatabaseBuilder();
                var hello = await databaseBuilder.GetHelloAsync(runner.Connection, runner.Transaction);
                await runner.CommitAsync().ConfigureAwait(false);
                return (context, hello.DatabaseName, hello.Version);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Get a snapshot root directory name and folder directory name
        /// </summary>
        public virtual Task<(string DirectoryRoot, string DirectoryName)> GetSnapshotDirectoryAsync(SyncParameters syncParameters = null, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => GetSnapshotDirectoryAsync(SyncOptions.DefaultScopeName, syncParameters, cancellationToken, progress);

        /// <summary>
        /// Get a snapshot root directory name and folder directory name
        /// </summary>
        public virtual Task<(string DirectoryRoot, string DirectoryName)> GetSnapshotDirectoryAsync(string scopeName, SyncParameters syncParameters = null, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => this.InternalGetSnapshotDirectoryPathAsync(scopeName, syncParameters, cancellationToken, progress);

        /// <summary>
        /// Internal routine to clean tmp folders. MUST be compare also with Options.CleanFolder
        /// </summary>
        internal virtual async Task<bool> InternalCanCleanFolderAsync(string scopeName, SyncParameters parameters, BatchInfo batchInfo,
                             CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var batchInfoDirectoryFullPath = new DirectoryInfo(batchInfo.GetDirectoryFullPath());

            var (snapshotRootDirectory, snapshotNameDirectory) = await this.GetSnapshotDirectoryAsync(scopeName, parameters);

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
        internal virtual Task<(string DirectoryRoot, string DirectoryName)>
            InternalGetSnapshotDirectoryPathAsync(string scopeName, SyncParameters parameters = null,
                             CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            if (string.IsNullOrEmpty(this.Options.SnapshotsDirectory))
                return Task.FromResult<(string, string)>((default, default));

            // cleansing scope name
            var directoryScopeName = new string(scopeName.Where(char.IsLetterOrDigit).ToArray());

            var directoryFullPath = Path.Combine(this.Options.SnapshotsDirectory, directoryScopeName);

            var sb = new StringBuilder();
            var underscore = "";

            if (parameters != null)
            {
                foreach (var p in parameters.OrderBy(p => p.Name))
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


        /// <summary>
        /// Gets the inner provider if any
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            if (this.Provider == null)
                return base.ToString();

            return $"{Provider.GetDatabaseName()}, {Provider.GetShortProviderTypeName()}";
        }

    }
}
