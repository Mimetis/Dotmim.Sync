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
        public SyncOptions Options { get; set; }


        /// <summary>
        /// set the progress action used to get progression on the provider
        /// </summary>
        public void SetProgress(IProgress<ProgressArgs> progress) => this.progress = progress;

        /// <summary>
        /// Set the cancellation token used to cancel sync
        /// </summary>
        /// <param name="token"></param>
        public void SetCancellationToken(CancellationToken token) => this.cancellationToken = token;


        /// <summary>
        /// Shortcut to raise a rollback error
        /// </summary>
        internal static void RaiseRollbackException(SyncContext context, string message) =>
            throw new SyncException(message, context.SyncStage, SyncExceptionType.Rollback);

        /// <summary>
        /// Try to raise a generalist progress event
        /// </summary>
        private void ReportProgress(SyncContext context, DbConnection connection = null, DbTransaction transaction = null)
        {
            if (this.progress == null)
                return;

            var args = new ProgressArgs(context, connection, transaction);

            this.progress.Report(args);

            if (args.Action == ChangeApplicationAction.Rollback)
                RaiseRollbackException(context, "Rollback by user during a progress event");
        }


        /// <summary>
        /// Subscribe an apply changes failed action
        /// </summary>
        public void InterceptApplyChangesFailed(Func<ApplyChangesFailedArgs, Task> action)
            => this.GetInterceptor<ApplyChangesFailedArgs>().Set(action);


        /// <summary>
        /// Get an interceptor for a given args, deriving from BaseArgs
        /// </summary>
        public InterceptorWrapper<T> GetInterceptor<T>() where T : BaseArgs
        {
            InterceptorWrapper<T> interceptor = null;
            var typeofT = typeof(T);

            // try get the interceptor from the dictionary and cast it
            if (this.dictionary.TryGetValue(typeofT, out var i))
                interceptor = (InterceptorWrapper<T>)i;

            // if null, create a new one
            if (interceptor == null)
            {
                interceptor = new InterceptorWrapper<T>();
                this.dictionary.Add(typeofT, interceptor);
            }

            return interceptor;
        }

        /// <summary>
        /// Reset all interceptors
        /// </summary>
        public void InterceptNone()
        {
            foreach(var interceptor in this.dictionary.Values)
            {
                interceptor.Dispose();
            }

            this.dictionary.Clear();
        }

        /// <summary>
        /// Returns the Task associated with given type of BaseArgs 
        /// Because we are not doing anything else than just returning a task, no need to use async / await. Just return the Task itself
        /// </summary>
        internal Task InterceptAsync<T>(T args) where T : BaseArgs
        {
            var interceptor = GetInterceptor<T>();
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

                // Set stage
                context.SyncStage = SyncStage.BeginSession;

                // Event progress
                this.ReportProgress(context);

                // Launch any interceptor if available
                await this.InterceptAsync(new SessionBeginArgs(context, null, null));

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

            // Event progress
            this.ReportProgress(context);

            // Launch any interceptor if available
            await this.InterceptAsync(new SessionEndArgs(context, null, null));

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
                    await connection.OpenAsync();
                    var scopeBuilder = this.GetScopeBuilder();
                    var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(message.ScopeInfoTableName, connection);
                    var localTime = scopeInfoBuilder.GetLocalTimestamp();
                    return (context, localTime);
                }
                finally
                {
                    if (connection.State != ConnectionState.Closed)
                        connection.Close();
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
