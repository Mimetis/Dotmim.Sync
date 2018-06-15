using Dotmim.Sync.Batch;
using Dotmim.Sync.Filter;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using Dotmim.Sync.Messages;

namespace Dotmim.Sync
{

    /// <summary>
    /// Sync agent. It's the sync orchestrator
    /// Knows both the Sync Server provider and the Sync Client provider
    /// </summary>
    public class SyncAgent : IDisposable
    {

        /// <summary>
        /// Gets or Sets the configuration object.
        /// This object will be passed to the remote provider, and will be handle by it.
        /// </summary>
        public SyncConfiguration Configuration { get; private set; }

        /// <summary>
        /// Defines the state that a synchronization session is in.
        /// </summary>
        public SyncSessionState SessionState { get; set; }

        /// <summary>
        /// Gets or Sets the provider for the Client Side
        /// </summary>
        public IProvider LocalProvider { get; set; }

        /// <summary>
        /// Get or Sets the provider for the Server Side
        /// </summary>
        public IProvider RemoteProvider { get; set; }

        // Scope informaitons. 
        // On Server, we have tow scopes available : Server Scope and Client (server timestamp) scope
        // On Client, we have only the client scope
        public Dictionary<string, ScopeInfo> Scopes { get; set; }

        /// <summary>
        /// Get or Sets the Sync parameter to pass to Remote provider for filtering rows
        /// </summary>
        public SyncParameterCollection Parameters { get; set; } = new SyncParameterCollection();


        // Read only SyncProgress for all SyncStage
        public event EventHandler<ProgressEventArgs> SyncProgress = null;

        // Types events for each SyncStage
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
        /// Occurs when a conflict is raised on the server side
        /// </summary>
        public event EventHandler<ApplyChangeFailedEventArgs> ApplyChangedFailed = null;

        /// <summary>
        /// Occurs when sync is starting, ending
        /// </summary>
        public event EventHandler<SyncSessionState> SessionStateChanged = null;



        /// <summary>
        /// SyncAgent manage both server and client provider
        /// It's the main object to launch the Sync process
        /// </summary>
        public SyncAgent(string scopeName, IProvider localProvider, IProvider remoteProvider)
        {
            this.LocalProvider = localProvider ?? throw new ArgumentNullException("ClientProvider");
            this.RemoteProvider = remoteProvider ?? throw new ArgumentNullException("ServerProvider");

            this.Configuration = new SyncConfiguration
            {
                ScopeName = scopeName ?? throw new ArgumentNullException("scopeName")
            };

            this.LocalProvider.SyncProgress += (s, e) => this.SyncProgress?.Invoke(s, e);
            this.LocalProvider.BeginSession += (s, e) => this.BeginSession?.Invoke(s, e);
            this.LocalProvider.EndSession += (s, e) => this.EndSession?.Invoke(s, e);
            this.LocalProvider.TableChangesApplied += (s, e) => this.TableChangesApplied?.Invoke(s, e);
            this.LocalProvider.TableChangesApplying += (s, e) => this.TableChangesApplying?.Invoke(s, e);
            this.LocalProvider.TableChangesSelected += (s, e) => this.TableChangesSelected?.Invoke(s, e);
            this.LocalProvider.TableChangesSelecting += (s, e) => this.TableChangesSelecting?.Invoke(s, e);
            this.LocalProvider.ConfigurationApplied += (s, e) => this.ConfigurationApplied?.Invoke(s, e);
            this.LocalProvider.ConfigurationApplying += (s, e) => this.ConfigurationApplying?.Invoke(s, e);
            this.LocalProvider.DatabaseApplied += (s, e) => this.DatabaseApplied?.Invoke(s, e);
            this.LocalProvider.DatabaseApplying += (s, e) => this.DatabaseApplying?.Invoke(s, e);
            this.LocalProvider.DatabaseTableApplied += (s, e) => this.DatabaseTableApplied?.Invoke(s, e);
            this.LocalProvider.DatabaseTableApplying += (s, e) => this.DatabaseTableApplying?.Invoke(s, e);
            this.LocalProvider.ScopeLoading += (s, e) => this.ScopeLoading?.Invoke(s, e);
            this.LocalProvider.ScopeSaved += (s, e) => this.ScopeSaved?.Invoke(s, e);

            this.RemoteProvider.ApplyChangedFailed += RemoteProvider_ApplyChangedFailed;
        }


        /// <summary>
        /// SyncAgent used in a web proxy sync session. No need to set tables, it's done from the server web api side.
        /// </summary>
        public SyncAgent(IProvider localProvider, IProvider remoteProvider)
            : this("DefaultScope", localProvider, remoteProvider)
        {
        }

        /// <summary>
        /// SyncAgent manage both server and client provider
        /// the tables array represents the tables you want to sync
        /// Don't work on the proxy provider
        /// </summary>
        public SyncAgent(string scopeName, IProvider clientProvider, IProvider serverProvider, string[] tables)
        : this(scopeName, clientProvider, serverProvider)
        {
            if (tables == null || tables.Length <= 0)
                throw new ArgumentException("you need to pass at lease one table name");

            if (!(this.RemoteProvider is CoreProvider remoteCoreProvider))
                throw new ArgumentException("Since the remote provider is a web proxy, you have to configure the server side");

            if (!remoteCoreProvider.CanBeServerProvider)
                throw new NotSupportedException();

            foreach (var tbl in tables)
                this.Configuration.Add(tbl);
        }

        /// <summary>
        /// SyncAgent manage both server and client provider
        /// the tables array represents the tables you want to sync
        /// Don't work on the proxy provider
        /// </summary>
        public SyncAgent(IProvider clientProvider, IProvider serverProvider, string[] tables)
        : this("DefaultScope", clientProvider, serverProvider, tables)
        {
        }


        /// <summary>
        /// Launch a normal synchronization
        /// </summary>
        public async Task<SyncContext> SynchronizeAsync()
        {
            return await this.SynchronizeAsync(SyncType.Normal, CancellationToken.None);
        }

        /// <summary>
        /// Launch a normal synchronization with a cancellation token
        /// </summary>
        public async Task<SyncContext> SynchronizeAsync(CancellationToken cancellationToken)
        {
            return await this.SynchronizeAsync(SyncType.Normal, cancellationToken);
        }

        /// <summary>
        /// Launch a synchronization with the specified mode
        /// </summary>
        public async Task<SyncContext> SynchronizeAsync(SyncType syncType)
        {
            return await this.SynchronizeAsync(syncType, CancellationToken.None);

        }

        /// <summary>
        /// Launch a synchronization with the specified mode
        /// </summary>
        public async Task<SyncContext> SynchronizeAsync(SyncType syncType, CancellationToken cancellationToken)
        {
            // Context, used to back and forth data between servers
            SyncContext context = new SyncContext(Guid.NewGuid())
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

            ScopeInfo localScopeInfo = null,
                      serverScopeInfo = null,
                      localScopeReferenceInfo = null,
                      scope = null;

            Guid fromId = Guid.Empty;
            long lastSyncTS = 0L;
            bool isNew = true;

            try
            {
                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Setting the cancellation token
                this.LocalProvider.SetCancellationToken(cancellationToken);
                this.RemoteProvider.SetCancellationToken(cancellationToken);

                // ----------------------------------------
                // 0) Begin Session / Get the Configuration from remote provider
                //    If the configuration object is provided by the client, the server will be updated with it.
                // ----------------------------------------
                var messageBeginSession = new MessageBeginSession { SyncConfiguration = this.Configuration };

                (context, this.Configuration) = await this.RemoteProvider.BeginSessionAsync(context, messageBeginSession);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                messageBeginSession = new MessageBeginSession { SyncConfiguration = this.Configuration };
                // Locally, nothing really special. Eventually, editing the config object
                (context, this.Configuration) = await this.LocalProvider.BeginSessionAsync(context, messageBeginSession);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // ----------------------------------------
                // 1) Read scope info
                // ----------------------------------------

                // get the scope from local provider 
                List<ScopeInfo> localScopes;
                List<ScopeInfo> serverScopes;
                (context, localScopes) = await this.LocalProvider.EnsureScopesAsync(context,
                    this.Configuration.ScopeInfoTableName, this.Configuration.ScopeName);

                if (localScopes.Count != 1)
                    throw new Exception("On Local provider, we should have only one scope info");

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                localScopeInfo = localScopes[0];

                (context, serverScopes) = await this.RemoteProvider.EnsureScopesAsync(context,
                    this.Configuration.ScopeInfoTableName, this.Configuration.ScopeName, localScopeInfo.Id);

                if (serverScopes.Count != 2)
                    throw new Exception("On Remote provider, we should have two scopes (one for server and one for client side)");

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                serverScopeInfo = serverScopes.First(s => s.Id != localScopeInfo.Id);
                localScopeReferenceInfo = serverScopes.First(s => s.Id == localScopeInfo.Id);

                // ----------------------------------------
                // 2) Build Configuration Object
                // ----------------------------------------

                // Get Schema from remote provider
                (context, this.Configuration.Schema) = await this.RemoteProvider.EnsureSchemaAsync(context, this.Configuration.Schema);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Apply on local Provider
                (context, this.Configuration.Schema) = await this.LocalProvider.EnsureSchemaAsync(context, this.Configuration.Schema);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // ----------------------------------------
                // 3) Ensure databases are ready
                // ----------------------------------------

                // Server should have already the schema
                context = await this.RemoteProvider.EnsureDatabaseAsync(context, serverScopeInfo, this.Configuration.Schema, this.Configuration.Filters);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Client could have, or not, the tables
                context = await this.LocalProvider.EnsureDatabaseAsync(context, localScopeInfo, this.Configuration.Schema, this.Configuration.Filters);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // ----------------------------------------
                // 5) Get changes and apply them
                // ----------------------------------------
                BatchInfo clientBatchInfo;
                BatchInfo serverBatchInfo;

                ChangesSelected clientChangesSelected = null;
                ChangesSelected serverChangesSelected = null;
                ChangesApplied clientChangesApplied = null;
                ChangesApplied serverChangesApplied = null;

                // fromId : not really needed on this case, since updated / inserted / deleted row has marked null
                // otherwise, lines updated by server or others clients are already syncked
                fromId = localScopeInfo.Id;
                // lastSyncTS : get lines inserted / updated / deteleted after the last sync commited
                lastSyncTS = localScopeInfo.LastTimestamp;
                // isNew : If isNew, lasttimestamp is not correct, so grab all
                isNew = localScopeInfo.IsNewScope;
                //Direction set to Upload
                context.SyncWay = SyncWay.Upload;


                // Apply on the Server Side
                // Since we are on the server, 
                // we need to check the server client timestamp (not the client timestamp which is completely different)
                ConflictResolutionPolicy serverPolicy = this.Configuration.ConflictResolutionPolicy;
                ConflictResolutionPolicy clientPolicy = serverPolicy == ConflictResolutionPolicy.ServerWins ? ConflictResolutionPolicy.ClientWins : ConflictResolutionPolicy.ServerWins;


                scope = new ScopeInfo { Id = fromId, IsNewScope = isNew, LastTimestamp = lastSyncTS };
                (context, clientBatchInfo, clientChangesSelected) =
                    await this.LocalProvider.GetChangeBatchAsync(context, scope, this.Configuration.Schema, this.Configuration.DownloadBatchSizeInKB, this.Configuration.BatchDirectory, clientPolicy, this.Configuration.Filters);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();


                // fromId : When applying rows, make sure it's identified as applied by this client scope
                fromId = localScopeInfo.Id;
                // lastSyncTS : apply lines only if thye are not modified since last client sync
                lastSyncTS = localScopeReferenceInfo.LastTimestamp;
                // isNew : not needed
                isNew = false;
                scope = new ScopeInfo { Id = fromId, IsNewScope = isNew, LastTimestamp = lastSyncTS };

                (context, serverChangesApplied) =
                    await this.RemoteProvider.ApplyChangesAsync(context, scope, this.Configuration.Schema, serverPolicy, this.Configuration.UseBulkOperations, this.Configuration.ScopeInfoTableName, clientBatchInfo);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();
                // Get changes from server


                // get the archive if exists
                if (localScopeReferenceInfo.IsNewScope && !String.IsNullOrEmpty(this.Configuration.Archive))
                {
                    //// fromId : Make sure we don't select lines on server that has been already updated by the client
                    //fromId = localScopeInfo.Id;
                    //// lastSyncTS : apply lines only if thye are not modified since last client sync
                    //lastSyncTS = localScopeReferenceInfo.LastTimestamp;
                    //// isNew : make sure we take all lines if it's the first time we get 
                    //isNew = localScopeReferenceInfo.IsNewScope;
                    //scope = new ScopeInfo { Id = fromId, IsNewScope = isNew, LastTimestamp = lastSyncTS };
                    ////Direction set to Download
                    //context.SyncWay = SyncWay.Download;

                    //(context, serverBatchInfo, serverChangesSelected) = await this.RemoteProvider.GetArchiveAsync(context, scope);

                    //// fromId : When applying rows, make sure it's identified as applied by this server scope
                    //fromId = serverScopeInfo.Id;
                    //// lastSyncTS : apply lines only if they are not modified since last client sync
                    //lastSyncTS = localScopeInfo.LastTimestamp;
                    //// isNew : if IsNew, don't apply deleted rows from server
                    //isNew = localScopeInfo.IsNewScope;
                    //scope = new ScopeInfo { Id = fromId, IsNewScope = isNew, LastTimestamp = lastSyncTS };

                    //(context, clientChangesApplied) = await this.LocalProvider.ApplyArchiveAsync(context, scope, serverBatchInfo);

                    //// Here we have to change the localScopeInfo.LastTimestamp to the good one
                    //// last ts from archive
                    //localScopeReferenceInfo.LastTimestamp = [something from the archive];
                    //// we are not new anymore 
                    //localScopeReferenceInfo.IsNewScope = false;
                }


                // fromId : Make sure we don't select lines on server that has been already updated by the client
                fromId = localScopeInfo.Id;
                // lastSyncTS : apply lines only if thye are not modified since last client sync
                lastSyncTS = localScopeReferenceInfo.LastTimestamp;
                // isNew : make sure we take all lines if it's the first time we get 
                isNew = localScopeReferenceInfo.IsNewScope;
                scope = new ScopeInfo { Id = fromId, IsNewScope = isNew, LastTimestamp = lastSyncTS };
                //Direction set to Download
                context.SyncWay = SyncWay.Download;


                (context, serverBatchInfo, serverChangesSelected) =
                    await this.RemoteProvider.GetChangeBatchAsync(context, scope, this.Configuration.Schema, this.Configuration.DownloadBatchSizeInKB, this.Configuration.BatchDirectory, serverPolicy, this.Configuration.Filters);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Apply local changes

                // fromId : When applying rows, make sure it's identified as applied by this server scope
                fromId = serverScopeInfo.Id;
                // lastSyncTS : apply lines only if they are not modified since last client sync
                lastSyncTS = localScopeInfo.LastTimestamp;
                // isNew : if IsNew, don't apply deleted rows from server
                isNew = localScopeInfo.IsNewScope;
                scope = new ScopeInfo { Id = fromId, IsNewScope = isNew, LastTimestamp = lastSyncTS };

                (context, clientChangesApplied) =
                    await this.LocalProvider.ApplyChangesAsync(context, scope, this.Configuration.Schema, clientPolicy, this.Configuration.UseBulkOperations, this.Configuration.ScopeInfoTableName, serverBatchInfo);

                context.TotalChangesDownloaded = clientChangesApplied.TotalAppliedChanges;
                context.TotalChangesUploaded = clientChangesSelected.TotalChangesSelected;
                context.TotalSyncErrors = clientChangesApplied.TotalAppliedChangesFailed;

                long serverTimestamp, clientTimestamp;

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                (context, serverTimestamp) = await this.RemoteProvider.GetLocalTimestampAsync(context, this.Configuration.ScopeInfoTableName);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                (context, clientTimestamp) = await this.LocalProvider.GetLocalTimestampAsync(context, this.Configuration.ScopeInfoTableName);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                context.CompleteTime = DateTime.Now;

                serverScopeInfo.IsNewScope = false;
                localScopeReferenceInfo.IsNewScope = false;
                localScopeInfo.IsNewScope = false;

                serverScopeInfo.LastSync = context.CompleteTime;
                localScopeReferenceInfo.LastSync = context.CompleteTime;
                localScopeInfo.LastSync = context.CompleteTime;

                serverScopeInfo.IsLocal = true;
                localScopeReferenceInfo.IsLocal = false;

                context = await this.RemoteProvider.WriteScopesAsync(context, this.Configuration.ScopeInfoTableName,
                    new List<ScopeInfo> { serverScopeInfo, localScopeReferenceInfo });

                serverScopeInfo.IsLocal = false;
                localScopeInfo.IsLocal = true;

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                context = await this.LocalProvider.WriteScopesAsync(context, this.Configuration.ScopeInfoTableName,
                    new List<ScopeInfo> { localScopeInfo, serverScopeInfo });

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

            }
            catch (SyncException se)
            {
                Debug.WriteLine($"Sync Exception: {se.Message}. Type:{se.Type}. On provider: {se.ProviderName}.");
                throw;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Unknwon Exception: {ex.Message}.");
                throw new SyncException(ex, SyncStage.None, string.Empty);
            }
            finally
            {
                // End the current session
                context = await this.RemoteProvider.EndSessionAsync(context);
                context = await this.LocalProvider.EndSessionAsync(context);

                this.SessionState = SyncSessionState.Ready;
                this.SessionStateChanged?.Invoke(this, this.SessionState);
            }

            return context;
        }


        private void RemoteProvider_ApplyChangedFailed(object sender, ApplyChangeFailedEventArgs e)
        {
            this.ApplyChangedFailed?.Invoke(this, e);
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
            this.LocalProvider.BeginSession -= (s, e) => this.BeginSession?.Invoke(s, e);
            this.LocalProvider.EndSession -= (s, e) => this.EndSession?.Invoke(s, e);
            this.LocalProvider.TableChangesApplied -= (s, e) => this.TableChangesApplied?.Invoke(s, e);
            this.LocalProvider.TableChangesApplying -= (s, e) => this.TableChangesApplying?.Invoke(s, e);
            this.LocalProvider.TableChangesSelected -= (s, e) => this.TableChangesSelected?.Invoke(s, e);
            this.LocalProvider.TableChangesSelecting -= (s, e) => this.TableChangesSelecting?.Invoke(s, e);
            this.LocalProvider.ConfigurationApplied -= (s, e) => this.ConfigurationApplied?.Invoke(s, e);
            this.LocalProvider.ConfigurationApplying -= (s, e) => this.ConfigurationApplying?.Invoke(s, e);
            this.LocalProvider.DatabaseApplied -= (s, e) => this.DatabaseApplied?.Invoke(s, e);
            this.LocalProvider.DatabaseApplying -= (s, e) => this.DatabaseApplying?.Invoke(s, e);
            this.LocalProvider.ScopeLoading -= (s, e) => this.ScopeLoading?.Invoke(s, e);
            this.LocalProvider.ScopeSaved -= (s, e) => this.ScopeSaved?.Invoke(s, e);

            this.RemoteProvider.ApplyChangedFailed -= RemoteProvider_ApplyChangedFailed;
        }
    }
}
