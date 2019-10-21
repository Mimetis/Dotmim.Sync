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
        private InterceptorBase interceptorBase;

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
        /// Shortcut to raise a rollback error
        /// </summary>
        internal static void RaiseRollbackException(SyncContext context, string message) =>
            throw new SyncException(message, context.SyncStage, SyncExceptionType.Rollback);

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

            var progressArgs = new ProgressArgs(context, message, connection, transaction);

            progress.Report(progressArgs);

            if (progressArgs.Action == ChangeApplicationAction.Rollback)
                RaiseRollbackException(context, "Rollback by user during a progress event");
        }




        /// <summary>
        /// Called by the  to indicate that a 
        /// synchronization session has started.
        /// </summary>
        public virtual async Task<SyncContext> BeginSessionAsync(SyncContext context,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            try
            {

                context.SyncStage = SyncStage.BeginSession;

                // Progress & interceptor
                var sessionArgs = new SessionBeginArgs(context, null, null);
                this.ReportProgress(context, progress, sessionArgs);
                await this.InterceptAsync(sessionArgs).ConfigureAwait(false);

                return context;
            }
            catch (Exception ex)
            {
                throw new SyncException(ex, SyncStage.BeginSession);
            }


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
        public virtual (SyncContext, long) GetLocalTimestampAsync(SyncContext context, MessageTimestamp message,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            var scopeBuilder = this.GetScopeBuilder();
            var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(message.ScopeInfoTableName, connection, transaction);
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
