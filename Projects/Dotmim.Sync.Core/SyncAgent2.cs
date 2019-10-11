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
    public class SyncAgent2 : IDisposable
    {
        /// <summary>
        /// Defines the state that a synchronization session is in.
        /// </summary>
        public SyncSessionState SessionState { get; set; }

        /// <summary>
        /// Gets or Sets the local orchestrator
        /// </summary>
        public ILocalOrchestrator<IProvider> LocalOrchestrator { get; set; }

        /// <summary>
        /// Get or Sets the remote orchestrator
        /// </summary>
        public IRemoteOrchestrator<IProvider> RemoteOrchestrator { get; set; }

        // Scope informaitons. 
        // On Server, we have tow scopes available : Server Scope and Client (server timestamp) scope
        // On Client, we have only the client scope
        public Dictionary<string, ScopeInfo> Scopes { get; set; }

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
        public void SetConfiguration(Action<SyncConfiguration> configuration)
            => this.LocalOrchestrator.SetConfiguration(configuration);

        /// <summary>
        /// Set Sync Options parameters
        /// </summary>
        public void SetOptions(Action<SyncOptions> options)
            => this.LocalOrchestrator.SetOptions(options);


        /// <summary>
        /// set the progress action used to get progression on the provider
        /// </summary>
        public void SetProgress(IProgress<ProgressArgs> progress)
            => this.LocalOrchestrator.SetProgress(progress);

        /// <summary>
        /// Set an interceptor to get info on the current sync process
        /// </summary>
        public void SetInterceptor(InterceptorBase interceptor)
            => this.LocalOrchestrator.On(interceptor);


        /// <summary>
        /// SyncAgent manage both server and client provider
        /// the tables array represents the tables you want to sync
        /// Don't work on the proxy provider
        /// </summary>
        public SyncAgent2(string scopeName, CoreProvider clientProvider, IProvider serverProvider, string[] tables)
        {
            if (tables == null || tables.Length <= 0)
                throw new ArgumentException("you need to pass at lease one table name");

            if (string.IsNullOrEmpty(scopeName))
                throw new ArgumentNullException("scopeName");

            this.LocalOrchestrator = new LocalOrchestrator();
            this.LocalOrchestrator.SetProvider(clientProvider);

            this.RemoteProvider = remoteProvider ?? throw new ArgumentNullException("ServerProvider");

            this.LocalProvider.SetConfiguration(c => c.ScopeName = scopeName);
            this.RemoteProvider.SetConfiguration(c => c.ScopeName = scopeName);

            this.Parameters = new SyncParameterCollection();



            if (!(this.RemoteProvider is CoreProvider remoteCoreProvider))
                throw new ArgumentException("Since the remote provider is a web proxy, you have to configure the server side");

            if (!remoteCoreProvider.CanBeServerProvider)
                throw new NotSupportedException();

            this.LocalProvider.SetConfiguration(c =>
            {
                foreach (var tbl in tables)
                    c.Add(tbl);
            });
            this.RemoteProvider.SetConfiguration(c =>
            {
                foreach (var tbl in tables)
                    c.Add(tbl);
            });
        }

        /// <summary>
        /// Launch a synchronization with the specified mode
       /*
        FROM:
            -------------------------------------------
            0) BeginSessionAsync : Was passing configuration between Client and Server
            -------------------------------------------
            FIRST ROUNDTRIP
            -------------------------------------------
            1) EnsureScopesAsync : Ensure scopes is created on both sides
                - Raise error if local scope is not equals to 1: On Local provider, we should have only one scope info
                - Write client scope to remote provider
                - Raise error if more than 2 scopes on remote On Remote provider, we should have two scopes(one for server and one for client side)
            -------------------------------------------
            SECOND ROUNDTRIP
            -------------------------------------------
            2) EnsureSchemaAsync : Get schema from remote and save locally
            -------------------------------------------
            THIRD ROUNDTRIP
            -------------------------------------------
            3) EnsureDatabaseAsync : Create the database remotelly and locally
            -------------------------------------------
            FOURTH ROUNDTRIP
            -------------------------------------------
            4) GetLocalTimestampAsync              
                - JUST before the whole process, get the timestamp, to be sure to get rows inserted / updated elsewhere since the sync is not over
            5) GetChangeBatchAsync locally
            6) ApplyChangesAsync remotely
            -------------------------------------------
            FIFTH ROUNDTRIP
            -------------------------------------------
            7) GetLocalTimestampAsync remotely
            8) GetChangeBatchAsync remotely
            9) ApplyChangesAsync locally
            -------------------------------------------
            SIXTH ROUNDTRIP
            -------------------------------------------
            10) Write scopes
                
            -------------------------------------------
            -------------------------------------------
            TO:
            -------------------------------------------
            0) BeginSessionAsync[Client] : session is beginning
                - Raise events
            1) EnsureScopeAsync[Client]: Be sure we have at least the scope table
            -------------------------------------------
            FIRST ROUNDTRIP
            -------------------------------------------
            2) GetChangeBatchAsync[Client]
            3) ApplyChangesOnServerAndGetChanges :
                EnsureScopesAsync[Server]
                GetSchemaAsync[Server]
                ApplyChangesAsync[Server]
                GetChangeBatchAsync[Server]
                WriteScopesAsync[Server]
            4) ApplySchema[Client]
            4) ApplyChangesAsync[Client]
            5) WriteScopesAsync[Client]

            IProvider
             - BeginSessionAsync
             - GetSchemaAsync
             - ApplySchemaAsync
             - EnsureScopesAsync
             - GetChangeBatchAsync
             - ApplyChangesAsync
             - WriteScopesAsync
             - EndSessionAsync


            ClientProvider(IProvider)
            - Implements all

            IRemoteProvider
            - Task<object> GetChangesAsync(object localChangesToApplyOnRemote);

            ILocalProvider
             - Task<object> GetChangesAsync();
             - Task<string> ApplyChangesAsync(object remoteChangesToApplyOnLocal);
            

            */
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

            ScopeInfo localScopeInfo = null,
                      serverScopeInfo = null,
                      localScopeReferenceInfo = null,
                      scope = null;

            var fromId = Guid.Empty;
            var lastSyncTS = 0L;
            var isNew = true;

            try
            {
                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Setting progress
                this.LocalOrchestrator.SetProgress(progress);

                SyncConfiguration conf = null;
                // ----------------------------------------
                // 0) Begin Session 
                // ----------------------------------------
                // Locally, nothing really special. Eventually, editing the config object
                await this.LocalOrchestrator.BeginSessionAsync(context, new MessageBeginSession { Configuration = conf }).ConfigureAwait(false);

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // ----------------------------------------
                // 1) Read scope info
                // ----------------------------------------

                // get the scope from local provider 
                List<ScopeInfo> localScopes;
                (context, localScopes) = await this.LocalProvider.EnsureScopesAsync(context,
                                    new MessageEnsureScopes
                                    {
                                        ScopeInfoTableName = this.LocalProvider.Configuration.ScopeInfoTableName,
                                        ScopeName = this.LocalProvider.Configuration.ScopeName,
                                        SerializationFormat = this.LocalProvider.Configuration.SerializationFormat
                                    }).ConfigureAwait(false);

                if (localScopes.Count != 1)
                    throw new Exception("On Local provider, we should have only one scope info");

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                localScopeInfo = localScopes[0];

                // ----------------------------------------
                // 5) Get local changes
                // ----------------------------------------
                BatchInfo clientBatchInfo;
                BatchInfo serverBatchInfo;

                DatabaseChangesSelected clientChangesSelected = null;
                DatabaseChangesSelected serverChangesSelected = null;
                DatabaseChangesApplied clientChangesApplied = null;
                DatabaseChangesApplied serverChangesApplied = null;

                var serverPolicy = this.LocalProvider.Configuration.ConflictResolutionPolicy;
                var clientPolicy = serverPolicy == ConflictResolutionPolicy.ServerWins ? ConflictResolutionPolicy.ClientWins : ConflictResolutionPolicy.ServerWins;

                scope = new ScopeInfo { Id = fromId, IsNewScope = isNew, Timestamp = lastSyncTS };
                (context, clientBatchInfo, clientChangesSelected) =
                    await this.LocalProvider.GetChangeBatchAsync(context,
                        new MessageGetChangesBatch
                        {
                            ScopeInfo = scope,
                            Schema = this.LocalProvider.Configuration.Schema,
                            Policy = clientPolicy,
                            Filters = this.LocalProvider.Configuration.Filters,
                            SerializationFormat = this.LocalProvider.Configuration.SerializationFormat
                        }).ConfigureAwait(false);

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
