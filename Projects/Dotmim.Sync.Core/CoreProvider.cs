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
    public abstract partial class CoreProvider
    {

        // Collection of Interceptors
        private Interceptors interceptors = new Interceptors();


        /// <summary>
        /// Connection is opened. this method is called before any interceptors
        /// </summary>
        public virtual void OnConnectionOpened(DbConnection connection) { }

        /// <summary>
        /// Connection is closed. this method is called after all interceptors
        /// </summary>
        public virtual void OnConnectionClosed(DbConnection connection) { }


        /// <summary>
        /// Gets or Sets options used during the sync
        /// </summary>
        public virtual SyncOptions Options { get; set; }

        /// <summary>
        /// Set an interceptor to get info on the current sync process
        /// </summary>
        public void On<T>(Func<T, Task> interceptorFunc) where T : ProgressArgs =>
            this.interceptors.GetInterceptor<T>().Set(interceptorFunc);

        /// <summary>
        /// Set an interceptor to get info on the current sync process
        /// </summary>
        public void On<T>(Action<T> interceptorAction) where T : ProgressArgs =>
            this.interceptors.GetInterceptor<T>().Set(interceptorAction);


        /// <summary>
        /// Set a collection of interceptors
        /// </summary>
        public void On(Interceptors interceptors) => this.interceptors = interceptors;

        /// <summary>
        /// Returns the Task associated with given type of BaseArgs 
        /// Because we are not doing anything else than just returning a task, no need to use async / await. Just return the Task itself
        /// </summary>
        public   Task InterceptAsync<T>(T args) where T : ProgressArgs
        {
            if (this.interceptors == null)
                return Task.CompletedTask;

            var interceptor = this.interceptors.GetInterceptor<T>();
            return interceptor.RunAsync(args);
        }

        /// <summary>
        /// Create a new instance of the implemented Connection provider
        /// </summary>
        public abstract DbConnection CreateConnection();

        /// <summary>
        /// Get a table builder helper. Need a complete table description (SchemaTable). Will then generate table, table tracking, stored proc and triggers
        /// </summary>
        /// <returns></returns>
        public abstract DbBuilder GetDatabaseBuilder(SyncTable tableDescription);

        /// <summary>
        /// Get a table manager, which can get informations directly from data source
        /// </summary>
        public abstract DbTableManagerFactory GetTableManagerFactory(string tableName, string schemaName);

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
        /// Try to report progress
        /// </summary>
        private void ReportProgress(SyncContext context, IProgress<ProgressArgs> progress, ProgressArgs args, DbConnection connection = null, DbTransaction transaction = null)
        {
            if (connection == null && args.Connection != null)
                connection = args.Connection;

            if (transaction == null && args.Transaction != null)
                transaction = args.Transaction;

            ReportProgress(context, progress, args.Message, connection, transaction);
        }

        /// <summary>
        /// Try to report progress
        /// </summary>
        private void ReportProgress(SyncContext context, IProgress<ProgressArgs> progress, string message, DbConnection connection = null, DbTransaction transaction = null)
        {
            if (progress == null)
                return;
            var dt = DateTime.Now;
            message = $"{dt.ToLongTimeString()}.{dt.Millisecond}\t {message}";
            var progressArgs = new ProgressArgs(context, message, connection, transaction);

            progress.Report(progressArgs);

            if (progressArgs.Action == ChangeApplicationAction.Rollback)
                throw new RollbackException("Rollback by user during a progress event");
        }


        /// <summary>
        /// Let a chance to provider to enrich SyncExecption
        /// </summary>
        public virtual void EnsureSyncException(SyncException syncException) { }

        /// <summary>
        /// Called by the  to indicate that a 
        /// synchronization session has started.
        /// </summary>
        public virtual async Task<SyncContext> BeginSessionAsync(SyncContext context,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            context.SyncStage = SyncStage.BeginSession;

            // Progress & interceptor
            var sessionArgs = new SessionBeginArgs(context, null, null);
            this.ReportProgress(context, progress, sessionArgs);
            await this.InterceptAsync(sessionArgs).ConfigureAwait(false);

            return context;

        }

        /// <summary>
        /// Called when the sync is over
        /// </summary>
        public virtual async Task<SyncContext> EndSessionAsync(SyncContext context,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            context.SyncStage = SyncStage.EndSession;

            // Progress & interceptor
            var sessionArgs = new SessionEndArgs(context, null, null);
            this.ReportProgress(context, progress, sessionArgs);
            await this.InterceptAsync(sessionArgs).ConfigureAwait(false);


            return context;
        }

        /// <summary>
        /// Read a scope info
        /// </summary>
        public virtual (SyncContext, long) GetLocalTimestampAsync(SyncContext context,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            var scopeBuilder = this.GetScopeBuilder();
            // Create a scopeInfo builder based on default scope inf table, since we don't use it to retrieve local time stamp, even if scope info table
            // in client database is not the DefaultScopeInfoTableName
            var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(SyncOptions.DefaultScopeInfoTableName, connection, transaction);
            var localTime = scopeInfoBuilder.GetLocalTimestamp();
            return (context, localTime);
        }

        /// <summary>
        /// TODO : Manager le fait qu'un scope peut être out dater, car il n'a pas synchronisé depuis assez longtemps
        /// </summary>
        internal virtual bool IsRemoteOutdated() =>
            //var lastCleanupTimeStamp = 0; // A établir comment récupérer la dernière date de clean up des metadatas
            //return (ScopeInfo.LastTimestamp < lastCleanupTimeStamp);
            false;
    }
}
