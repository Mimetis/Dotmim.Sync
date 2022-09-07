
using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
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
        /// All changes are serialized on disk and can be load in memory from the <c>ServerBatchInfo</c> property (of type <see cref="BatchInfo"/>)
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
        public virtual async Task<ServerSyncChanges> GetChangesAsync(ScopeInfoClient cScopeInfoClient)
        {

            var context = new SyncContext(Guid.NewGuid(), cScopeInfoClient);

            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesSelecting).ConfigureAwait(false);

                // Before getting changes, be sure we have a remote schema available
                ScopeInfo sScopeInfo;
                (context, sScopeInfo) = await this.InternalGetScopeInfoAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // Should we ?
                if (sScopeInfo.Schema == null)
                    throw new MissingRemoteOrchestratorSchemaException();

                //Direction set to Download
                context.SyncWay = SyncWay.Download;

                // Output
                // JUST Before get changes, get the timestamp, to be sure to 
                // get rows inserted / updated elsewhere since the sync is not over
                long remoteClientTimestamp;
                (context, remoteClientTimestamp) = await this.InternalGetLocalTimestampAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress);


                BatchInfo serverBatchInfo;
                DatabaseChangesSelected serverChangesSelected;
                // When we get the chnages from server, we create the batches if it's requested by the client
                // the batch decision comes from batchsize from client
                (context, serverBatchInfo, serverChangesSelected) =
                    await this.InternalGetChangesAsync(sScopeInfo, context, cScopeInfoClient.IsNewScope, cScopeInfoClient.LastServerSyncTimestamp, remoteClientTimestamp,
                    cScopeInfoClient.Id, this.Provider.SupportsMultipleActiveResultSets,
                    this.Options.BatchDirectory, null, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return new ServerSyncChanges(remoteClientTimestamp, serverBatchInfo, serverChangesSelected, null);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
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
        public virtual async Task<ServerSyncChanges> GetEstimatedChangesCountAsync(ScopeInfoClient cScopeInfoClient)
        {

            var context = new SyncContext(Guid.NewGuid(), cScopeInfoClient);

            try
            {

                ScopeInfo sScopeInfo;
                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesSelecting).ConfigureAwait(false);
               
                (context, sScopeInfo) = await this.InternalGetScopeInfoAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
               
                // Should we ?
                if (sScopeInfo.Schema == null)
                    throw new MissingRemoteOrchestratorSchemaException();

                //Direction set to Download
                context.SyncWay = SyncWay.Download;

                // Output
                // JUST Before get changes, get the timestamp, to be sure to 
                // get rows inserted / updated elsewhere since the sync is not over
                long remoteClientTimestamp;
                (context, remoteClientTimestamp) = await this.InternalGetLocalTimestampAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress);

                DatabaseChangesSelected serverChangesSelected;
                // When we get the chnages from server, we create the batches if it's requested by the client
                // the batch decision comes from batchsize from client
                (context, serverChangesSelected) =
                    await this.InternalGetEstimatedChangesCountAsync(sScopeInfo, context, cScopeInfoClient.IsNewScope, cScopeInfoClient.LastServerSyncTimestamp,
                    remoteClientTimestamp, cScopeInfoClient.Id, this.Provider.SupportsMultipleActiveResultSets, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                var serverSyncChanges = new ServerSyncChanges(remoteClientTimestamp, null, serverChangesSelected, null);

                return serverSyncChanges;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }
    }
}
