using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Cache;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Log;
using Dotmim.Sync.Manager;
using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using Dotmim.Sync.Serialization;
using System.Diagnostics;
using System.Text;

namespace Dotmim.Sync
{
    /// <summary>
    /// Core provider : should be implemented by any server / client provider
    /// </summary>
    public abstract partial class CoreProvider : IProvider
    {
        private const string SYNC_CONF = "syncconf";

        private bool syncInProgress;
        private CancellationToken cancellationToken;

        /// <summary>
        /// Raise an event if the sync is outdated. 
        /// Let the user choose if he wants to force or not
        /// </summary>
        public event EventHandler<OutdatedEventArgs> SyncOutdated = null;

        public event EventHandler<ProgressEventArgs> SyncProgress = null;
        public event EventHandler<BeginSessionEventArgs> BeginSession = null;
        public event EventHandler<EndSessionEventArgs> EndSession = null;
        public event EventHandler<ScopeEventArgs> ScopeLoading = null;
        public event EventHandler<ScopeEventArgs> ScopeSaved = null;
        public event EventHandler<DatabaseApplyingEventArgs> DatabaseApplying = null;
        public event EventHandler<DatabaseAppliedEventArgs> DatabaseApplied = null;
        public event EventHandler<DatabaseTableApplyingEventArgs> DatabaseTableApplying = null;
        public event EventHandler<DatabaseTableAppliedEventArgs> DatabaseTableApplied = null;
        public event EventHandler<ConfigurationApplyingEventArgs> ConfigurationApplying = null;
        public event EventHandler<ConfigurationAppliedEventArgs> ConfigurationApplied = null;
        public event EventHandler<TableChangesSelectingEventArgs> TableChangesSelecting = null;
        public event EventHandler<TableChangesSelectedEventArgs> TableChangesSelected = null;
        public event EventHandler<TableChangesApplyingEventArgs> TableChangesApplying = null;
        public event EventHandler<TableChangesAppliedEventArgs> TableChangesApplied = null;

        /// <summary>
        /// Occurs when a conflict is raised.
        /// </summary>
        public event EventHandler<ApplyChangeFailedEventArgs> ApplyChangedFailed = null;

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
        /// Try to raise a specific progress event
        /// </summary>
        private void TryRaiseProgressEvent<T>(T args, EventHandler<T> handler) where T : BaseProgressEventArgs
        {
            args.Action = ChangeApplicationAction.Continue;

            handler?.Invoke(this, args);

            if (args.Action == ChangeApplicationAction.Rollback)
                throw new RollbackException();

            var props = new Dictionary<String, String>();

            switch (args.Stage)
            {
                case SyncStage.None:
                    break;
                case SyncStage.BeginSession:
                    this.TryRaiseProgressEvent(SyncStage.BeginSession, $"Begin session");
                    break;
                case SyncStage.ScopeLoading:
                    props.Add("ScopeId", (args as ScopeEventArgs).ScopeInfo.Id.ToString());
                    this.TryRaiseProgressEvent(SyncStage.ScopeLoading, $"Loading scope", props);
                    break;
                case SyncStage.ScopeSaved:
                    props.Add("ScopeId", (args as ScopeEventArgs).ScopeInfo.Id.ToString());
                    this.TryRaiseProgressEvent(SyncStage.ScopeLoading, $"Scope saved", props);
                    break;
                case SyncStage.ConfigurationApplying:
                    this.TryRaiseProgressEvent(SyncStage.ConfigurationApplying, $"Applying configuration");
                    break;
                case SyncStage.ConfigurationApplied:
                    this.TryRaiseProgressEvent(SyncStage.ConfigurationApplied, $"Configuration applied");
                    break;
                case SyncStage.DatabaseApplying:
                    this.TryRaiseProgressEvent(SyncStage.DatabaseApplying, $"Applying database schemas");
                    break;
                case SyncStage.DatabaseApplied:
                    props.Add("Script", (args as DatabaseAppliedEventArgs).Script);
                    this.TryRaiseProgressEvent(SyncStage.DatabaseApplied, $"Database schemas applied", props);
                    break;
                case SyncStage.DatabaseTableApplying:
                    props.Add("TableName", (args as DatabaseTableApplyingEventArgs).TableName);
                    this.TryRaiseProgressEvent(SyncStage.DatabaseApplying, $"Applying schema table", props);
                    break;
                case SyncStage.DatabaseTableApplied:
                    props.Add("TableName", (args as DatabaseTableAppliedEventArgs).TableName);
                    props.Add("Script", (args as DatabaseTableAppliedEventArgs).Script);
                    this.TryRaiseProgressEvent(SyncStage.DatabaseApplied, $"Table schema applied", props);
                    break;
                case SyncStage.TableChangesSelecting:
                    props.Add("TableName", (args as TableChangesSelectingEventArgs).TableName);
                    this.TryRaiseProgressEvent(SyncStage.TableChangesSelecting, $"Selecting changes", props);
                    break;
                case SyncStage.TableChangesSelected:
                    props.Add("TableName", (args as TableChangesSelectedEventArgs).TableChangesSelected.TableName);
                    props.Add("Deletes", (args as TableChangesSelectedEventArgs).TableChangesSelected.Deletes.ToString());
                    props.Add("Inserts", (args as TableChangesSelectedEventArgs).TableChangesSelected.Inserts.ToString());
                    props.Add("Updates", (args as TableChangesSelectedEventArgs).TableChangesSelected.Updates.ToString());
                    props.Add("TotalChanges", (args as TableChangesSelectedEventArgs).TableChangesSelected.TotalChanges.ToString());
                    this.TryRaiseProgressEvent(SyncStage.TableChangesSelected, $"Changes selected", props);
                    break;
                case SyncStage.TableChangesApplying:
                    props.Add("TableName", (args as TableChangesApplyingEventArgs).TableName);
                    props.Add("State", (args as TableChangesApplyingEventArgs).State.ToString());
                    this.TryRaiseProgressEvent(SyncStage.TableChangesApplying, $"Applying changes", props);
                    break;
                case SyncStage.TableChangesApplied:
                    props.Add("TableName", (args as TableChangesAppliedEventArgs).TableChangesApplied.TableName);
                    props.Add("State", (args as TableChangesAppliedEventArgs).TableChangesApplied.State.ToString());
                    props.Add("Applied", (args as TableChangesAppliedEventArgs).TableChangesApplied.Applied.ToString());
                    props.Add("Failed", (args as TableChangesAppliedEventArgs).TableChangesApplied.Failed.ToString());
                    this.TryRaiseProgressEvent(SyncStage.TableChangesApplied, $"Changes applied", props);
                    break;
                case SyncStage.EndSession:
                    this.TryRaiseProgressEvent(SyncStage.EndSession, $"End session");
                    break;
                case SyncStage.CleanupMetadata:
                    break;
            }
        }

        /// <summary>
        /// Try to raise a generalist progress event
        /// </summary>
        private void TryRaiseProgressEvent(SyncStage stage, String message, Dictionary<String, String> properties = null)
        {
            ProgressEventArgs progressEventArgs = new ProgressEventArgs(this.ProviderTypeName, stage, message);

            if (properties != null)
                progressEventArgs.Properties = properties;

            SyncProgress?.Invoke(this, progressEventArgs);

            if (progressEventArgs.Action == ChangeApplicationAction.Rollback)
                throw new RollbackException();
        }

        /// <summary>
        /// Called by the  to indicate that a 
        /// synchronization session has started.
        /// </summary>
        public virtual Task<(SyncContext, SyncConfiguration)> BeginSessionAsync(SyncContext context, SyncConfiguration configuration)
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
                // TODO : First step to edit the configuration
                var progressEventArgs = new BeginSessionEventArgs(this.ProviderTypeName, context.SyncStage);
                this.TryRaiseProgressEvent(progressEventArgs, this.BeginSession);

                return Task.FromResult((context, configuration));
            }
            catch (Exception ex)
            {
                throw new SyncException(ex, SyncStage.BeginSession, this.ProviderTypeName);
            }

            
        }

        /// <summary>
        /// Called when the sync is over
        /// </summary>
        public virtual Task<SyncContext> EndSessionAsync(SyncContext context)
        {
            // already ended
            lock (this)
            {
                if (!syncInProgress)
                    return Task.FromResult(context);
            }

            context.SyncStage = SyncStage.EndSession;

            // Event progress
            this.TryRaiseProgressEvent(
                new EndSessionEventArgs(this.ProviderTypeName, context.SyncStage), this.EndSession);

            lock (this)
            {
                this.syncInProgress = false;
            }

            return Task.FromResult(context);
        }

        /// <summary>
        /// Read a scope info
        /// </summary>
        public virtual async Task<(SyncContext, long)> GetLocalTimestampAsync(SyncContext context, string scopeInfoTableName)
        {
            // Open the connection
            using (var connection = this.CreateConnection())
            {
                try
                {
                    await connection.OpenAsync();
                    var scopeBuilder = this.GetScopeBuilder();
                    var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(scopeInfoTableName, connection);
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
        internal virtual bool IsRemoteOutdated()
        {
            //var lastCleanupTimeStamp = 0; // A établir comment récupérer la dernière date de clean up des metadatas
            //return (ScopeInfo.LastTimestamp < lastCleanupTimeStamp);

            return false;
        }

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


        public void SetCancellationToken(CancellationToken token)
        {
            this.cancellationToken = token;
        }
    }
}
