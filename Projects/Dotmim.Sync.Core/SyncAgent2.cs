using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Filter;
using Dotmim.Sync.Messages;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Sync agent. It's the sync orchestrator
    /// Knows both the Sync Server provider and the Sync Client provider
    /// </summary>
    public class SyncAgent : IDisposable
    {
        private SyncSchema schema = new SyncSchema();
        private SyncOptions options = new SyncOptions();
        private IProgress<ProgressArgs> remoteProgress = null;

        /// <summary>
        /// Defines the state that a synchronization session is in.
        /// </summary>
        public SyncSessionState SessionState { get; set; }

        /// <summary>
        /// Gets or Sets the local orchestrator
        /// </summary>
        public ILocalOrchestrator LocalOrchestrator { get; set; }

        /// <summary>
        /// Get or Sets the remote orchestrator
        /// </summary>
        public IRemoteOrchestrator RemoteOrchestrator { get; set; }

        /// <summary>
        /// Get or Sets the Sync parameter to pass to Remote provider for filtering rows
        /// </summary>
        public SyncParameterCollection Parameters { get; private set; }

        /// <summary>
        /// Occurs when sync is starting, ending
        /// </summary>
        public event EventHandler<SyncSessionState> SessionStateChanged = null;

        /// <summary>
        /// Set Sync Configuration parameters
        /// </summary>
        public void SetSchema(Action<SyncSchema> onSchema)
            => onSchema?.Invoke(this.schema);

        /// <summary>
        /// Set Sync Options parameters
        /// </summary>
        public void SetOptions(Action<SyncOptions> onOptions)
            => onOptions?.Invoke(this.options);

        public void AddFilter(FilterClause filter)
        {
            if (!this.schema.Filters.Any(f => f.TableName == filter.TableName && f.ColumnName == filter.ColumnName))
                this.schema.Filters.Add(filter);
        }

        public void SetInterceptors(Interceptors interceptors)
            => this.LocalOrchestrator.On(interceptors);



        /// <summary>
        /// If you want to see remote progress as well (only on direct connection)
        /// </summary>
        /// <param name="remoteProgress"></param>
        public void AddRemoteProgress(IProgress<ProgressArgs> remoteProgress)
        {
            this.remoteProgress = remoteProgress;
        }

        /// <summary>
        /// Shortcut to Apply changed failed (supported by the local orchestrator)
        /// </summary>
        public void OnApplyChangesFailed(Func<ApplyChangesFailedArgs, Task> func) => this.LocalOrchestrator.OnApplyChangesFailed(func);

        /// <summary>
        /// Shortcut to Apply changed failed (supported by the local orchestrator)
        /// </summary>
        public void OnApplyChangesFailed(Action<ApplyChangesFailedArgs> action) => this.LocalOrchestrator.OnApplyChangesFailed(action);


        public SyncAgent(string scopeName, CoreProvider clientProvider, CoreProvider serverProvider, string[] tables = null)
            : this(scopeName, new LocalOrchestrator(clientProvider), new RemoteOrchestrator(serverProvider), tables)
        {
        }
        public SyncAgent(CoreProvider clientProvider, CoreProvider serverProvider, string[] tables = null)
            : this(SyncOptions.DefaultScopeName, clientProvider, serverProvider, tables)
        {
        }
        public SyncAgent(string scopeName, CoreProvider clientProvider, IRemoteOrchestrator remoteOrchestrator, string[] tables = null)
            : this(scopeName, new LocalOrchestrator(clientProvider), remoteOrchestrator, tables)
        {
        }
        public SyncAgent(CoreProvider clientProvider, IRemoteOrchestrator remoteOrchestrator, string[] tables = null)
            : this(SyncOptions.DefaultScopeName, new LocalOrchestrator(clientProvider), remoteOrchestrator, tables)
        {
        }
        
        public SyncAgent(ILocalOrchestrator localOrchestrator, IRemoteOrchestrator remoteOrchestrator, string[] tables = null)
            : this(SyncOptions.DefaultScopeName, localOrchestrator, remoteOrchestrator, tables)
        {
        }

        public SyncAgent(string scopeName, ILocalOrchestrator localOrchestrator, IRemoteOrchestrator remoteOrchestrator, string[] tables = null)
        {
            if (string.IsNullOrEmpty(scopeName))
                throw new ArgumentNullException("scopeName");

            if (remoteOrchestrator.Provider != null && !remoteOrchestrator.Provider.CanBeServerProvider)
                throw new NotSupportedException();

            this.LocalOrchestrator = localOrchestrator;
            this.RemoteOrchestrator = remoteOrchestrator;

            this.SetSchema(c =>
            {
                c.ScopeName = scopeName;

                if (tables != null && tables.Length > 0)
                    foreach (var tbl in tables)
                        c.Add(tbl);
            });

            this.Parameters = new SyncParameterCollection();
        }


        /// <summary>
        /// Launch a normal synchronization without any IProgess or CancellationToken
        /// </summary>
        public Task<SyncContext> SynchronizeAsync()
        {
            return SynchronizeAsync(SyncType.Normal, CancellationToken.None);
        }


        /// <summary>
        /// Launch a synchronization with the specified mode
        /// </summary>
        public async Task<SyncContext> SynchronizeAsync(SyncType syncType,
            CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            // Context, used to back and forth data between servers
            var context = new SyncContext(Guid.NewGuid())
            {
                // set start time
                StartTime = DateTime.Now,
                // if any parameters, set in context
                Parameters = this.Parameters,
                // set sync type (Normal, Reinitialize, ReinitializeWithUpload)
                SyncType = syncType
            };

            this.SessionState = SyncSessionState.Synchronizing;
            this.SessionStateChanged?.Invoke(this, this.SessionState);

            try
            {
                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Starts sync by :
                // - Getting local config we have set by code
                // - Ensure local scope is created (table and values)
                var clientScope = await this.LocalOrchestrator.EnsureScopeAsync
                        (context, schema, options, cancellationToken, progress);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                context = clientScope.context;

                // FIRST call to server
                // Get the server scope info and server reference id to local scope
                // Be sure options / schema from client are passed if needed
                // Then the configuration with full schema
                var serverScope = await this.RemoteOrchestrator.EnsureScopeAsync(
                        context, schema, options, clientScope.localScopeInfo.Id,
                        cancellationToken, remoteProgress);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                context = serverScope.context;
                this.schema = serverScope.schema;


                // on local orchestrator, get local changes
                // Most probably the schema has changed, so we passed it again (coming from Server)
                // Don't need to pass again Options since we are not modifying it between server and client
                var clientChanges = await this.LocalOrchestrator.GetChangesAsync(
                    context, schema, clientScope.localScopeInfo, serverScope.serverScopeInfo,
                    cancellationToken, progress);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // set context
                context = clientChanges.context;

                // SECOND call to server
                var serverChanges = await this.RemoteOrchestrator.ApplyThenGetChangesAsync(
                    context, clientScope.localScopeInfo.Id, serverScope.localScopeReferenceInfo,
                    serverScope.serverScopeInfo, clientChanges.clientBatchInfo,
                    cancellationToken, remoteProgress);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // set context
                context = serverChanges.context;

                var localChanges = await this.LocalOrchestrator.ApplyChangesAsync(
                    context, clientChanges.clientTimestamp,
                    serverScope.serverScopeInfo.Id, clientScope.localScopeInfo, serverChanges.serverBatchInfo,
                    cancellationToken, progress);


                context.TotalChangesDownloaded = localChanges.clientChangesApplied.TotalAppliedChanges;
                context.TotalChangesUploaded = clientChanges.clientChangesSelected.TotalChangesSelected;
                context.TotalSyncErrors = localChanges.clientChangesApplied.TotalAppliedChangesFailed;

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

            }
            catch (SyncException se)
            {
                Console.WriteLine($"Sync Exception: {se.Message}. Type:{se.Type}.");
                throw;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unknwon Exception: {ex.Message}.");
                throw new SyncException(ex, SyncStage.None);
            }
            finally
            {
                // End the current session
                this.SessionState = SyncSessionState.Ready;
                this.SessionStateChanged?.Invoke(this, this.SessionState);
            }

            return context;
        }


        // --------------------------------------------------------------------
        // Dispose
        // --------------------------------------------------------------------

        /// <summary>
        /// Releases all resources used by the <see cref="T:Microsoft.Synchronization.Data.DbSyncBatchInfo" />.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Releases the unmanaged resources used 
        /// by the <see cref="T:Microsoft.Synchronization.Data.DbSyncBatchInfo" /> and optionally releases the managed resources.
        /// </summary>
        protected virtual void Dispose(bool cleanup)
        {

        }
    }
}
