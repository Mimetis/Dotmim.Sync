using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Contains the logic to get changes from the server side.
    /// </summary>
    public partial class RemoteOrchestrator : BaseOrchestrator
    {

        /// <summary>
        /// Get changes from <strong>server</strong> datasource to be send to a particular <strong>client</strong>.
        /// <para>
        /// You need an instance of <see cref="ScopeInfoClient"/> (containing all required info) from the client side
        /// to be able to get changes from the server side.
        /// </para>
        /// <example>
        /// Example:
        /// <code>
        ///  var localOrchestrator = new LocalOrchestrator(clientProvider);
        ///  var remoteOrchestrator = new RemoteOrchestrator(remoteProvider);
        ///  var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName, parameters);
        ///  // You can load a client scope info from the server database also, if you know the clientId
        ///  // var cScopeInfoClient = await remoteOrchestrator.GetScopeInfoClientAsync(clientId, scopeName, parameters);
        ///  var changes = await remoteOrchestrator.GetChangesAsync(cScopeInfoClient);
        /// </code>
        /// </example>
        /// </summary>
        /// <returns>
        /// Returns a <see cref="ServerSyncChanges"/> instance.
        /// <para>
        /// All changes are serialized on disk and can be load in memory from the <c>ServerBatchInfo</c> property (of type <see cref="BatchInfo"/>).
        /// </para>
        /// <example>
        /// You can load in memory the changes using the <c>LoadTableFromBatchInfoAsync()</c> method:
        /// <code>
        /// var productCategoryTable = await remoteOrchestrator.LoadTableFromBatchInfoAsync(
        ///     scopeName, changes.ClientBatchInfo, "ProductCategory");
        ///
        /// foreach (var productCategoryRow in productCategoryTable.Rows)
        /// {
        ///    ....
        /// }
        /// </code>
        /// </example>
        /// </returns>
        public virtual async Task<ServerSyncChanges> GetChangesAsync(ScopeInfoClient cScopeInfoClient, DbConnection connection = null, DbTransaction transaction = null)
        {
            Guard.ThrowIfNull(cScopeInfoClient);
            var context = new SyncContext(Guid.NewGuid(), cScopeInfoClient);

            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesSelecting, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    // Before getting changes, be sure we have a remote schema available
                    ScopeInfo sScopeInfo;
                    (context, sScopeInfo) = await this.InternalGetScopeInfoAsync(context, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    // Should we ?
                    if (sScopeInfo.Schema == null)
                        throw new MissingRemoteOrchestratorSchemaException();

                    // Direction set to Download
                    context.SyncWay = SyncWay.Download;

                    // Output
                    // JUST Before get changes, get the timestamp, to be sure to
                    // get rows inserted / updated elsewhere since the sync is not over
                    long remoteClientTimestamp;
                    (context, remoteClientTimestamp) = await this.InternalGetLocalTimestampAsync(context, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    // Create a batch info
                    var info = connection != null && !string.IsNullOrEmpty(connection.Database) ? $"{connection.Database}_REMOTE_GETCHANGES" : "REMOTE_GETCHANGES";
                    var serverBatchInfo = new BatchInfo(this.Options.BatchDirectory, info: info);

                    // Call interceptor
                    var databaseChangesSelectingArgs = new DatabaseChangesSelectingArgs(context, serverBatchInfo.GetDirectoryFullPath(), this.Options.BatchSize, true,
                        cScopeInfoClient.LastServerSyncTimestamp, remoteClientTimestamp,
                        runner.Connection, runner.Transaction);

                    await this.InterceptAsync(databaseChangesSelectingArgs, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    // When we get the chnages from server, we create the batches if it's requested by the client
                    // the batch decision comes from batchsize from client
                    var serverChangesSelected = await this.InternalGetChangesAsync(sScopeInfo, context, cScopeInfoClient.IsNewScope, cScopeInfoClient.LastServerSyncTimestamp, remoteClientTimestamp,
                        cScopeInfoClient.Id, this.Provider.SupportsMultipleActiveResultSets, serverBatchInfo,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    await runner.CommitAsync().ConfigureAwait(false);

                    var databaseChangesSelectedArgs = new DatabaseChangesSelectedArgs(context, cScopeInfoClient.LastServerSyncTimestamp, remoteClientTimestamp,
                    serverBatchInfo, serverChangesSelected, runner.Connection, runner.Transaction);
                    await this.InterceptAsync(databaseChangesSelectedArgs, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    return new ServerSyncChanges(remoteClientTimestamp, serverBatchInfo, serverChangesSelected, null);
                }
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Get <strong>an estimation count</strong> of the changes from <strong>server</strong> datasource to be send to a particular <strong>client</strong>.
        /// <para>
        /// You need an instance of <see cref="ScopeInfoClient"/> (containing all required info) from the client side
        /// to be able to get changes from the server side.
        /// </para>
        /// <example>
        /// Example:
        /// <code>
        ///  var localOrchestrator = new LocalOrchestrator(clientProvider);
        ///  var remoteOrchestrator = new RemoteOrchestrator(remoteProvider);
        ///  var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName, parameters);
        ///  // You can load a client scope info from the server database also, if you know the clientId
        ///  var cScopeInfoClient = await remoteOrchestrator.GetScopeInfoClientAsync(clientId, scopeName, parameters);
        ///  var estimatedChanges = await remoteOrchestrator.GetEstimatedChangesCountAsync(cScopeInfoClient);
        /// </code>
        /// </example>
        /// </summary>
        /// <returns>
        /// Returns a <see cref="ServerSyncChanges"/> instance.
        /// <para>
        /// No changes are downloaded, so far the <c>ServerBatchInfo</c> property is always <c>null</c>.
        /// </para>
        /// The propery <c>ServerChangesSelected</c> (of type <see cref="DatabaseChangesSelected"/>)
        /// contains an estimation count of the changes from your server datsource for
        /// all the tables from your setup.
        /// </returns>
        public virtual async Task<ServerSyncChanges> GetEstimatedChangesCountAsync(ScopeInfoClient cScopeInfoClient, DbConnection connection = null, DbTransaction transaction = null)
        {

            Guard.ThrowIfNull(cScopeInfoClient);
            var context = new SyncContext(Guid.NewGuid(), cScopeInfoClient);

            try
            {

                ScopeInfo sScopeInfo;
                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesSelecting, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {

                    (context, sScopeInfo) = await this.InternalGetScopeInfoAsync(context, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    // Should we ?
                    if (sScopeInfo.Schema == null)
                        throw new MissingRemoteOrchestratorSchemaException();

                    // Direction set to Download
                    context.SyncWay = SyncWay.Download;

                    // Output
                    // JUST Before get changes, get the timestamp, to be sure to
                    // get rows inserted / updated elsewhere since the sync is not over
                    long remoteClientTimestamp;
                    (context, remoteClientTimestamp) = await this.InternalGetLocalTimestampAsync(context, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    DatabaseChangesSelected serverChangesSelected;

                    // When we get the chnages from server, we create the batches if it's requested by the client
                    // the batch decision comes from batchsize from client
                    (context, serverChangesSelected) =
                        await this.InternalGetEstimatedChangesCountAsync(sScopeInfo, context, cScopeInfoClient.IsNewScope, cScopeInfoClient.LastServerSyncTimestamp,
                        remoteClientTimestamp, cScopeInfoClient.Id, this.Provider.SupportsMultipleActiveResultSets, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    var serverSyncChanges = new ServerSyncChanges(remoteClientTimestamp, null, serverChangesSelected, null);

                    return serverSyncChanges;
                }
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }
    }
}