using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Contains the logic to handle client scope info.
    /// </summary>
    public partial class RemoteOrchestrator : BaseOrchestrator
    {
        /// <summary>
        /// Get a Scope Info Client from Server database.
        /// <para>
        /// Client should have already made a sync to be present in the server database scope_info_client table.
        /// </para>
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(serverProvider);
        /// var cScopeInfoClient = await remoteOrchestrator.GetScopeInfoClientAsync(clientId, default, parameters);
        /// </code>
        /// </example>
        /// </summary>
        public virtual async Task<ScopeInfoClient> GetScopeInfoClientAsync(Guid clientId, string scopeName = SyncOptions.DefaultScopeName, SyncParameters parameters = default,
            DbConnection connection = default, DbTransaction transaction = default)
        {
            // Create context
            var context = new SyncContext(Guid.NewGuid(), scopeName)
            {
                ClientId = clientId,
                Parameters = parameters,
            };

            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ScopeLoading, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    bool exists;
                    (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (!exists)
                    {
                        await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                    }

                    // Get scope if exists
                    ScopeInfoClient scopeInfoClient;
                    (context, scopeInfoClient) = await this.InternalLoadScopeInfoClientAsync(
                        context,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    await runner.CommitAsync().ConfigureAwait(false);

                    return scopeInfoClient;
                }
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }
    }
}