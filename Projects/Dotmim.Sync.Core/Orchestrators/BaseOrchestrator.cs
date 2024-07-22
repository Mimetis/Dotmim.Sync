using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Contains internals methods and properties for the orchestrator.
    /// </summary>
    public abstract partial class BaseOrchestrator
    {
        /// <summary>
        /// Gets a static serializer globally used by all orchestrators.
        /// </summary>
        internal static ISerializer Serializer => SerializersFactory.JsonSerializerFactory.GetSerializer();

        /// <summary>
        /// Gets all the interceptors for this orchestrator.
        /// </summary>
        internal Interceptors Interceptors { get; } = new();

        /// <summary>
        /// Initializes a new instance of the <see cref="BaseOrchestrator"/> class.
        /// </summary>
        internal BaseOrchestrator(CoreProvider provider, SyncOptions options)
        {
            this.Options = options ?? throw this.GetSyncError(null, new ArgumentNullException(nameof(options)));

            if (provider != null)
            {
                this.Provider = provider;
                this.Provider.Orchestrator = this;
            }

            this.Logger = options.Logger;
        }

        /// <summary>
        /// Gets or Sets the provider used by this local orchestrator.
        /// </summary>
        public virtual CoreProvider Provider { get; set; }

        /// <summary>
        /// Gets the options used by this local orchestrator.
        /// </summary>
        public virtual SyncOptions Options { get; internal set; }

        /// <summary>
        /// Gets or Sets the end time for this orchestrator.
        /// </summary>
        public virtual DateTime? CompleteTime { get; set; }

        /// <summary>
        /// Gets or Sets the logger used by this orchestrator.
        /// </summary>
        public virtual ILogger Logger { get; set; }

        /// <summary>
        /// Remove all Interceptors based on type of ProgressArgs.
        /// </summary>
        public void ClearInterceptors<T>()
            where T : ProgressArgs
            => this.Interceptors.Clear<T>();

        /// <summary>
        /// Remove all Interceptors.
        /// </summary>
        public void ClearInterceptors() => this.Interceptors.Clear();

        /// <summary>
        /// Remove interceptor based on Id.
        /// </summary>
        public void ClearInterceptors(Guid id) => this.Interceptors.Clear(id);

        /// <summary>
        /// Returns a boolean value indicating if we have any Interceptors for the current type T.
        /// </summary>
        public bool HasInterceptors<T>()
            where T : ProgressArgs
        {
            if (this.Interceptors == null)
                return false;

            var interceptors = this.Interceptors.GetInterceptors<T>();

            return interceptors.Count != 0;
        }

        /// <summary>
        /// Get the provider sync adapter.
        /// </summary>
        public virtual DbSyncAdapter GetSyncAdapter(string scopeName, SyncTable tableDescription, SyncSetup setup = default)
        {
            // var p = this.Provider.GetParsers(tableDescription, setup);

            // var data = JsonSerializer.SerializeToUtf8Bytes(setup);
            // var hash = HashAlgorithm.SHA256.Create(data);
            // var hashString = Convert.ToBase64String(hash);

            //// Create the key
            // var commandKey = $"{p.tableName.ToString()}-{p.trackingName.ToString()}-{hashString}-{this.Provider.ConnectionString}";

            //// Get a lazy command instance
            // var lazySyncAdapter = syncAdapters.GetOrAdd(commandKey,
            //    k => new Lazy<DbSyncAdapter>(() => this.Provider.GetSyncAdapter(tableDescription, setup)));

            //// Get the concrete instance
            // var syncAdapter = lazySyncAdapter.Value;

            // return syncAdapter;
            if (this.Provider == null)
                return null;

            var (tableName, trackingTableName) = this.Provider.GetParsers(tableDescription, setup);
            return this.Provider.GetSyncAdapter(tableDescription, tableName, trackingTableName, setup, scopeName);
        }

        /// <summary>
        /// Get the provider table builder.
        /// </summary>
        public DbTableBuilder GetTableBuilder(SyncTable tableDescription, ScopeInfo scopeInfo)
        {
            // var p = this.Provider.GetParsers(tableDescription, setup);

            // var data = JsonSerializer.SerializeToUtf8Bytes(setup);
            // var hash = HashAlgorithm.SHA256.Create(data);
            // var hashString = Convert.ToBase64String(hash);

            //// Create the key
            // var commandKey = $"{p.tableName.ToString()}-{p.trackingName.ToString()}-{hashString}-{this.Provider.ConnectionString}";

            //// Get a lazy command instance
            // var lazyTableBuilder = tableBuilders.GetOrAdd(commandKey,
            //    k => new Lazy<DbTableBuilder>(() => this.Provider.GetTableBuilder(tableDescription, setup)));

            //// Get the concrete instance
            // var tableBuilder = lazyTableBuilder.Value;

            // return tableBuilder;
            if (this.Provider == null)
                return null;

            Guard.ThrowIfNull(scopeInfo);

            var (tableName, trackingTableName) = this.Provider.GetParsers(tableDescription, scopeInfo.Setup);
            return this.Provider.GetTableBuilder(tableDescription, tableName, trackingTableName, scopeInfo.Setup, scopeInfo.Name);
        }

        /// <summary>
        /// Get a provider scope builder by scope table name.
        /// </summary>
        public DbScopeBuilder GetScopeBuilder(string scopeInfoTableName)
        {
            //// Create the key
            // var commandKey = $"{scopeInfoTableName}-{this.Provider.ConnectionString}";

            //// Get a lazy command instance
            // var lazyScopeBuilder = scopeBuilders.GetOrAdd(commandKey,
            //    k => new Lazy<DbScopeBuilder>(() => this.Provider.GetScopeBuilder(scopeInfoTableName)));

            //// Get the concrete instance
            // var scopeBuilder = lazyScopeBuilder.Value;

            // return scopeBuilder;
            if (this.Provider == null)
                return null;

            return this.Provider.GetScopeBuilder(scopeInfoTableName);
        }

        /// <summary>
        /// Check if a database exists, regarding the provider you are using. Returns database name and database version.
        /// </summary>
        public virtual Task<(SyncContext Context, string DatabaseName, string Version)> GetHelloAsync() => this.GetHelloAsync(SyncOptions.DefaultScopeName);

        /// <summary>
        /// Check if a database exists, regarding the provider you are using. Returns database name and database version.
        /// </summary>
        public virtual Task<(SyncContext Context, string DatabaseName, string Version)> GetHelloAsync(string scopeName)
            => this.InternalGetHelloAsync(new SyncContext(Guid.NewGuid(), scopeName), default, default, default, default);

        /// <summary>
        /// Get a snapshot root directory name and folder directory name.
        /// </summary>
        public virtual Task<(string DirectoryRoot, string DirectoryName)> GetSnapshotDirectoryAsync(SyncParameters syncParameters = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
            => this.GetSnapshotDirectoryAsync(SyncOptions.DefaultScopeName, syncParameters, progress, cancellationToken);

        /// <summary>
        /// Get a snapshot root directory name and folder directory name.
        /// </summary>
        public virtual Task<(string DirectoryRoot, string DirectoryName)> GetSnapshotDirectoryAsync(string scopeName, SyncParameters syncParameters = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
            => this.InternalGetSnapshotDirectoryPathAsync(scopeName, syncParameters, progress, cancellationToken);

        /// <summary>
        /// Gets the inner provider name if any.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            if (this.Provider == null)
                return base.ToString();

            return $"{this.Provider.GetDatabaseName()}, {this.Provider.GetShortProviderTypeName()}";
        }

        /// <summary>
        /// Add an interceptor of T.
        /// </summary>
        internal virtual Guid AddInterceptor<T>(Action<T> action)
            where T : ProgressArgs
            => this.Interceptors.Add(action);

        /// <summary>
        /// Add an async interceptor of T.
        /// </summary>
        internal virtual Guid AddInterceptor<T>(Func<T, Task> action)
            where T : ProgressArgs
            => this.Interceptors.Add(action);

        /// <summary>
        /// Try to proc a On[Method].
        /// </summary>
        internal async Task<T> InterceptAsync<T>(T args, IProgress<ProgressArgs> progress = default, CancellationToken cancellationToken = default)
            where T : ProgressArgs
        {
            if (this.Interceptors == null)
                return args;

            var interceptors = this.Interceptors.GetInterceptors<T>();

            // Check logger, because we make some reflection here
            if (this.Logger.IsEnabled(LogLevel.Debug))
            {
                // for example, getting DatabaseChangesSelectingArgs and transform to DatabaseChangesSelecting
#if NET6_0_OR_GREATER
                var argsTypeName = args.GetType().Name.Replace("Args", string.Empty, SyncGlobalization.DataSourceStringComparison);
#else
                var argsTypeName = args.GetType().Name.Replace("Args", string.Empty);
#endif
                this.Logger.LogDebug(SyncEventsId.CreateEventId(0, argsTypeName), args);

                this.Logger.LogDebug(new EventId(args.EventId, argsTypeName), args);
            }

            foreach (var interceptor in interceptors)
                await interceptor.RunAsync(args, cancellationToken).ConfigureAwait(false);

            if (progress != default)
                this.ReportProgress(args.Context, progress, args, args.Connection, args.Transaction);

            return args;
        }

        /// <summary>
        /// Try to report progress.
        /// </summary>
        internal void ReportProgress(SyncContext context, IProgress<ProgressArgs> progress, ProgressArgs args, DbConnection connection = null, DbTransaction transaction = null)
        {
            // Check logger, because we make some reflection here
            if (this.Logger.IsEnabled(LogLevel.Information))
            {
#if NET6_0_OR_GREATER
                var argsTypeName = args.GetType().Name.Replace("Args", string.Empty, SyncGlobalization.DataSourceStringComparison);
#else
                var argsTypeName = args.GetType().Name.Replace("Args", string.Empty);
#endif

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
        /// Open a connection.
        /// </summary>
        [DebuggerStepThrough]
        internal virtual async Task OpenConnectionAsync(SyncContext context, DbConnection connection, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
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
            await policy.ExecuteAsync(ct => connection.OpenAsync(ct), cancellationToken).ConfigureAwait(false);

            // Let provider knows a connection is opened
            this.Provider.onConnectionOpened(connection);

            await this.InterceptAsync(new ConnectionOpenedArgs(context, connection), progress, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Close a connection.
        /// </summary>
        internal virtual async Task CloseConnectionAsync(SyncContext context, DbConnection connection, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            if (this.Provider == null)
                return;

            if (connection != null && connection.State == ConnectionState.Closed)
                return;

            var isClosedHere = false;

            if (connection != null && connection.State == ConnectionState.Open)
            {
#if NET6_0_OR_GREATER
                await connection.CloseAsync().ConfigureAwait(false);
#else
                connection.Close();
#endif

                isClosedHere = true;
            }

            if (!cancellationToken.IsCancellationRequested)
                await this.InterceptAsync(new ConnectionClosedArgs(context, connection), progress, cancellationToken).ConfigureAwait(false);

            // Let provider knows a connection is closed
            this.Provider.onConnectionClosed(connection);

            if (isClosedHere && connection != null)
                connection.Dispose();
        }

        /// <summary>
        /// Check if the orchestrator database is outdated.
        /// </summary>
        internal virtual async Task<(SyncContext Context, bool IsOutDated)> InternalIsOutDatedAsync(SyncContext context, ScopeInfoClient cScopeInfoClient, ScopeInfo sScopeInfo, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            var isOutdated = false;

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
        /// Internal routine to clean tmp folders. MUST be compare also with Options.CleanFolder.
        /// </summary>
        internal virtual async Task<bool> InternalCanCleanFolderAsync(string scopeName, SyncParameters parameters, BatchInfo batchInfo,
                             IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            var batchInfoDirectoryFullPath = new DirectoryInfo(batchInfo.GetDirectoryFullPath());

            var (snapshotRootDirectory, snapshotNameDirectory) = await this.GetSnapshotDirectoryAsync(scopeName, parameters, cancellationToken: cancellationToken).ConfigureAwait(false);

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
        /// Internal routine to get the snapshot root directory and batch directory name.
        /// </summary>
        internal virtual Task<(string DirectoryRoot, string DirectoryName)>
            InternalGetSnapshotDirectoryPathAsync(string scopeName, SyncParameters parameters = null,
                             IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {

            if (string.IsNullOrEmpty(this.Options.SnapshotsDirectory))
                return Task.FromResult<(string, string)>((default, default));

            // cleansing scope name
            var directoryScopeName = new string(scopeName.Where(char.IsLetterOrDigit).ToArray());

            var directoryFullPath = Path.Combine(this.Options.SnapshotsDirectory, directoryScopeName);

            var sb = new StringBuilder();
            var underscore = string.Empty;

            if (parameters != null)
            {
                foreach (var p in parameters.OrderBy(p => p.Name))
                {
                    var value = "null";
                    if (p.Value is not null)
                    {
                        value = p.Value.ToString();
                    }

                    var cleanValue = new string(value.Where(char.IsLetterOrDigit).ToArray());
                    var cleanName = new string(p.Name.Where(char.IsLetterOrDigit).ToArray());

#pragma warning disable CA1305 // Specify IFormatProvider
                    sb.Append($"{underscore}{cleanName}_{cleanValue}");
#pragma warning restore CA1305 // Specify IFormatProvider
                    underscore = "_";
                }
            }

            var directoryName = sb.ToString();
            directoryName = string.IsNullOrEmpty(directoryName) ? "ALL" : directoryName;

            return Task.FromResult((directoryFullPath, directoryName));
        }

        /// <summary>
        /// Returns a new instance of <see cref="SyncException"/>  based on the current exception and optional message, from the method caller.
        /// </summary>
        internal SyncException GetSyncError(SyncContext context, Exception exception, string message = default,
            [CallerMemberName] string methodName = null)
        {
            // First we log the error before adding a new layer
            if (this.Logger != null)
                this.Logger.LogError(SyncEventsId.Exception, exception, exception.Message);

            // Get SyncStage, scopeName, methodName and message
            var strSyncStage = context == null ? SyncStage.None : context.SyncStage;
            var strMethodName = string.IsNullOrEmpty(methodName) ? string.Empty : $"[{methodName}].";

            message = $"{strMethodName}{message}.{exception.Message}";

            SyncException syncException;

            if (exception is SyncException)
            {
                syncException = new SyncException(exception.InnerException, message, strSyncStage);
            }
            else
            {
                syncException = new SyncException(exception, message, strSyncStage);

                // try to let the provider enrich the exception
                if (this.Provider != null)
                    this.Provider.EnsureSyncException(syncException);
            }

            return syncException;
        }

        /// <summary>
        /// Get hello from database.
        /// </summary>
        internal virtual async Task<(SyncContext Context, string DatabaseName, string Version)> InternalGetHelloAsync(
            SyncContext context,
            DbConnection connection = default, DbTransaction transaction = default, IProgress<ProgressArgs> progress = default, CancellationToken cancellationToken = default)
        {
            if (this.Provider == null)
                return (context, default, default);

            try
            {
                // TODO : get all scopes for Hello all of them
                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    var databaseBuilder = this.Provider.GetDatabaseBuilder();
                    var hello = await databaseBuilder.GetHelloAsync(runner.Connection, runner.Transaction).ConfigureAwait(false);
                    await runner.CommitAsync().ConfigureAwait(false);
                    return (context, hello.DatabaseName, hello.Version);
                }
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }
    }
}