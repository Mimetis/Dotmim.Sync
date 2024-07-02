using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public partial class LocalOrchestrator : BaseOrchestrator
    {
        /// <summary>
        /// Get a Scope Info Client
        /// </summary>
        public virtual async Task<ScopeInfoClient> GetScopeInfoClientAsync(string scopeName = SyncOptions.DefaultScopeName, SyncParameters syncParameters = default,
                                                                           DbConnection connection = null, DbTransaction transaction = null)
        {
            // Create context
            var context = new SyncContext(Guid.NewGuid(), scopeName)
            {
                Parameters = syncParameters
            };

            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ScopeLoading, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    // Get scope if exists
                    ScopeInfoClient scopeInfoClient;
                    (context, scopeInfoClient) = await this.InternalEnsureScopeInfoClientAsync(context,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    return scopeInfoClient;
                }
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }
        
        /// <summary>
        /// Get the client scope histories
        /// </summary>
        internal virtual async Task<(SyncContext context, ScopeInfoClient cScopeInfoClient)> InternalEnsureScopeInfoClientAsync(SyncContext context,
            DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    bool exists;
                    (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    if (!exists)
                        await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient,
                            runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    // load all scope info client
                    var cScopeInfoClients = await this.InternalLoadAllScopeInfoClientsAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    // Get scope info client if exists
                    ScopeInfoClient cScopeInfoClient = null;

                    if (context.ClientId.HasValue)
                        cScopeInfoClient = cScopeInfoClients.FirstOrDefault(sic => sic.Id == context.ClientId.Value && sic.Hash == context.Hash && sic.Name == context.ScopeName);
                    else
                        cScopeInfoClient = cScopeInfoClients.FirstOrDefault(sic => sic.Hash == context.Hash && sic.Name == context.ScopeName);

                    var shouldSave = false;

                    // Get scopeId representing the client unique id
                    if (cScopeInfoClient == null)
                    {
                        shouldSave = true;

                        cScopeInfoClient = this.InternalCreateScopeInfoClient(context.ScopeName, context.Parameters);

                        if (cScopeInfoClients != null && cScopeInfoClients.Count > 0)
                            cScopeInfoClient.Id = cScopeInfoClients[0].Id;
                    }

                    // affect correct value to current context
                    context.ClientId = cScopeInfoClient.Id;

                    if (shouldSave)
                        (context, cScopeInfoClient) = await this.InternalSaveScopeInfoClientAsync(cScopeInfoClient, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    return (context, cScopeInfoClient);
                }
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }
    }
}