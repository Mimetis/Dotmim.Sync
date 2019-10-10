using Dotmim.Sync.Builders;
using Dotmim.Sync.Cache;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using Dotmim.Sync.Messages;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Core provider : should be implemented by any server / client provider
    /// </summary>
    public abstract partial class CoreProvider : IProvider
    {
        private bool syncInProgress;
        private readonly Dictionary<Type, ISyncInterceptor> dictionary = new Dictionary<Type, ISyncInterceptor>();
        private CancellationToken cancellationToken;
        private IProgress<ProgressArgs> progress;
        private InterceptorBase interceptorBase;

        /// <summary>
        /// Create a new instance of the implemented Connection provider
        /// </summary>
        public abstract DbConnection CreateConnection();

        /// <summary>
        /// Get a table builder helper. Need a complete table description (DmTable). Will then generate table, table tracking, stored proc and triggers
        /// </summary>
        /// <returns></returns>
        public abstract DbBuilder GetDatabaseBuilder(DmTable tableDescription);

        /// <summary>
        /// Get a table manager, which can get informations directly from data source
        /// </summary>
        public abstract DbManager GetDbManager(string tableName);

        /// <summary>
        /// Create a Scope Builder, which can create scope table, and scope config
        /// </summary>
        public abstract DbScopeBuilder GetScopeBuilder();

        /// <summary>
        /// Gets or sets the metadata resolver (validating the columns definition from the data store)
        /// </summary>
        public abstract DbMetadata Metadata { get; set; }

        /// <summary>
        /// Get the cache manager. will store the configuration because we dont want to store it in database
        /// </summary>
        public abstract ICache CacheManager { get; set; }

        /// <summary>
        /// Get the provider type name
        /// </summary>
        public abstract string ProviderTypeName { get; }

        /// <summary>
        /// Gets or sets the connection string used by the implemented provider
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Gets a boolean indicating if the provider can use bulk operations
        /// </summary>
        public abstract bool SupportBulkOperations { get; }

        /// <summary>
        /// Gets a boolean indicating if the provider can be a server side provider
        /// </summary>
        public abstract bool CanBeServerProvider { get; }

        /// <summary>
        /// Gets the options used on this provider
        /// </summary>
        public SyncOptions Options { get; internal set; } = new SyncOptions();

        /// <summary>
        /// Gets the options used on this provider
        /// </summary>
        public SyncConfiguration Configuration { get; set; } = new SyncConfiguration();

        /// <summary>
        /// Set Options parameters
        /// </summary>
        public void SetOptions(Action<SyncOptions> options)
            => options?.Invoke(this.Options);

        /// <summary>
        /// Set Configuration parameters
        /// </summary>
        public void SetConfiguration(Action<SyncConfiguration> configuration)
            => configuration?.Invoke(this.Configuration);

        /// <summary>
        /// set the progress action used to get progression on the provider
        /// </summary>
        public void SetProgress(IProgress<ProgressArgs> progress)
            => this.progress = progress;

        /// <summary>
        /// Set an interceptor to get info on the current sync process
        /// </summary>
        public void On(InterceptorBase interceptor)
            => this.interceptorBase = interceptor;

        /// <summary>
        /// Set an interceptor to get info on the current sync process
        /// </summary>
        public void On<T>(Action<T> interceptorAction) where T : ProgressArgs
            => this.interceptorBase = new Interceptor<T>(interceptorAction);

        /// <summary>
        /// Set the cancellation token used to cancel sync
        /// </summary>
        public void SetCancellationToken(CancellationToken token)
            => this.cancellationToken = token;


        /// <summary>
        /// Shortcut to raise a rollback error
        /// </summary>
        internal static void RaiseRollbackException(SyncContext context, string message) =>
            throw new SyncException(message, context.SyncStage, SyncExceptionType.Rollback);

        /// <summary>
        /// Try to report progress
        /// </summary>
        private void ReportProgress(SyncContext context, ProgressArgs args, DbConnection connection = null, DbTransaction transaction = null)
        {
            if (connection == null && args.Connection != null)
                connection = args.Connection;

            if (transaction == null && args.Transaction != null)
                transaction = args.Transaction;

            ReportProgress(context, args.Message, connection, transaction);
        }

        /// <summary>
        /// Try to report progress
        /// </summary>
        private void ReportProgress(SyncContext context, string message, DbConnection connection = null, DbTransaction transaction = null)
        {
            if (this.progress == null)
                return;

            var progressArgs = new ProgressArgs(context, message, connection, transaction);

            this.progress.Report(progressArgs);

            if (progressArgs.Action == ChangeApplicationAction.Rollback)
                RaiseRollbackException(context, "Rollback by user during a progress event");
        }

        /// <summary>
        /// Returns the Task associated with given type of BaseArgs 
        /// Because we are not doing anything else than just returning a task, no need to use async / await. Just return the Task itself
        /// </summary>
        internal Task InterceptAsync<T>(T args) where T : ProgressArgs
        {
            if (this.interceptorBase == null)
                return Task.CompletedTask;

            var interceptor = this.interceptorBase.GetInterceptor<T>();
            return interceptor.RunAsync(args);
        }


        /// <summary>
        /// Called by the  to indicate that a 
        /// synchronization session has started.
        /// </summary>
        public virtual async Task<(SyncContext, SyncConfiguration)> BeginSessionAsync(SyncContext context, MessageBeginSession message)
        {
            try
            {
                lock (this)
                {
                    if (this.syncInProgress)
                        throw new InProgressException("Synchronization already in progress");

                    this.syncInProgress = true;
                }

                // Progress & interceptor
                context.SyncStage = SyncStage.BeginSession;
                var sessionArgs = new SessionBeginArgs(context, null, null);
                this.ReportProgress(context, sessionArgs);
                await this.InterceptAsync(sessionArgs).ConfigureAwait(false);

                return (context, message.Configuration);
            }
            catch (Exception ex)
            {
                throw new SyncException(ex, SyncStage.BeginSession);
            }


        }

        /// <summary>
        /// Called when the sync is over
        /// </summary>
        public virtual async Task<SyncContext> EndSessionAsync(SyncContext context)
        {
            // already ended
            lock (this)
            {
                if (!this.syncInProgress)
                    return context;
            }

            context.SyncStage = SyncStage.EndSession;

            // Progress & interceptor
            var sessionArgs = new SessionEndArgs(context, null, null);
            this.ReportProgress(context, sessionArgs);
            await this.InterceptAsync(sessionArgs).ConfigureAwait(false);

            lock (this)
            {
                this.syncInProgress = false;
            }

            return context;
        }

        /// <summary>
        /// Read a scope info
        /// </summary>
        public virtual async Task<(SyncContext, long)> GetLocalTimestampAsync(SyncContext context, MessageTimestamp message)
        {
            // Open the connection
            using (var connection = this.CreateConnection())
            {
                try
                {
                    await connection.OpenAsync().ConfigureAwait(false);
                    await this.InterceptAsync(new ConnectionOpenArgs(context, connection)).ConfigureAwait(false);

                    var scopeBuilder = this.GetScopeBuilder();
                    var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(message.ScopeInfoTableName, connection);
                    var localTime = scopeInfoBuilder.GetLocalTimestamp();
                    return (context, localTime);
                }
                finally
                {
                    if (connection.State != ConnectionState.Closed)
                        connection.Close();

                    await this.InterceptAsync(new ConnectionCloseArgs(context, connection, null)).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// TODO : Manager le fait qu'un scope peut être out dater, car il n'a pas synchronisé depuis assez longtemps
        /// </summary>
        internal virtual bool IsRemoteOutdated() =>
            //var lastCleanupTimeStamp = 0; // A établir comment récupérer la dernière date de clean up des metadatas
            //return (ScopeInfo.LastTimestamp < lastCleanupTimeStamp);

            false;

        /// <summary>
        /// Add metadata columns
        /// </summary>
        private void AddTrackingColumns<T>(DmTable table, string name)
        {
            if (!table.Columns.Contains(name))
            {
                var dc = new DmColumn<T>(name) { DefaultValue = default(T) };
                table.Columns.Add(dc);
            }
        }

        private void RemoveTrackingColumns(DmTable changes, string name)
        {
            if (changes.Columns.Contains(name))
                changes.Columns.Remove(name);
        }


    }
}
