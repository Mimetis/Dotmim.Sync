
using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
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

    public partial class LocalOrchestrator : BaseOrchestrator
    {

        /// <summary>
        /// Get changes from <strong>client</strong> datasource to be send to the <strong>server</strong>.
        /// <para>
        /// You need an instance of <see cref="ScopeInfoClient"/> (containing all required info) 
        /// to be able to get changes from your local datasource.
        /// </para>
        /// <example>
        /// Example:
        /// <code>
        ///  var localOrchestrator = new LocalOrchestrator(clientProvider);
        ///  var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName, parameters);
        ///  var changes = await localOrchestrator.GetChangesAsync(cScopeInfoClient);
        /// </code>
        /// </example>
        /// </summary>
        /// <returns>
        /// Returns a <see cref="ClientSyncChanges"/> instance.
        /// <para>
        /// All changes are serialized on disk and can be load in memory from the <c>ClientBatchInfo</c> property (of type <see cref="BatchInfo"/>)
        /// </para>
        /// <example>
        /// You can load in memory the changes using the <c>LoadTableFromBatchInfoAsync()</c> method:
        /// <code>
        /// var productCategoryTable = await localOrchestrator.LoadTableFromBatchInfoAsync(
        ///     scopeName, changes.ClientBatchInfo, "ProductCategory");
        ///     
        /// foreach (var productCategoryRow in productCategoryTable.Rows)
        /// {
        ///    ....
        /// }
        /// </code>
        /// </example>
        /// </returns>
        public virtual async Task<ClientSyncChanges> GetChangesAsync(ScopeInfoClient cScopeInfoClient)
        {
            var context = new SyncContext(Guid.NewGuid(), cScopeInfoClient.Name, cScopeInfoClient.Parameters)
            {
                ClientId = cScopeInfoClient.Id
            };

            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesSelecting).ConfigureAwait(false);

                ScopeInfo cScopeInfo;
                (context, cScopeInfo) = await this.InternalEnsureScopeInfoAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                ClientSyncChanges clientChanges = null;
                (context, clientChanges) = await this.InternalGetChangesAsync(cScopeInfo, context, cScopeInfoClient,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return clientChanges;

            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

        }

        /// <summary>
        /// Get <strong>an estimation count</strong> of the changes from <strong>client</strong> datasource to be send to the <strong>server</strong>.
        /// <para>
        /// You need an instance of <see cref="ScopeInfoClient"/> (containing all required info) 
        /// to be able to get the estimation count of the changes from your local datasource.
        /// </para>
        /// <example>
        /// Example:
        /// <code>
        ///  var localOrchestrator = new LocalOrchestrator(clientProvider);
        ///  var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName, parameters);
        ///  var changes = await localOrchestrator.GetEstimatedChangesCountAsync(cScopeInfoClient);
        /// </code>
        /// </example>
        /// </summary>
        /// <returns>
        /// Returns a <see cref="ClientSyncChanges"/> instance.
        /// <para>
        /// No changes are downloaded, so far the <c>ClientBatchInfo</c> property is always <c>null</c>.
        /// </para>
        /// The propery <c>ClientChangesSelected</c> (of type <see cref="DatabaseChangesSelected"/>) 
        /// contains an estimation count of the changes from your local datsource for
        /// all the tables from your setup.
        /// </returns>        
        public async Task<ClientSyncChanges> GetEstimatedChangesCountAsync(ScopeInfoClient cScopeInfoClient)
        {
            var context = new SyncContext(Guid.NewGuid(), cScopeInfoClient.Name, cScopeInfoClient.Parameters)
            {
                ClientId = cScopeInfoClient.Id
            };

            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesSelecting).ConfigureAwait(false);

                // Get the local setup & schema
                ScopeInfo cScopeInfo;
                (context, cScopeInfo) = await this.InternalEnsureScopeInfoAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (cScopeInfo.Schema == null)
                    return default;

                // On local, we don't want to chase rows from "others" 
                // We just want our local rows, so we dont exclude any remote scope id, by setting scope id to NULL
                Guid? remoteScopeId = null;
                var lastTimestamp = cScopeInfoClient.LastSyncTimestamp;
                var isNew = cScopeInfoClient.IsNewScope;

                //Direction set to Upload
                context.SyncWay = SyncWay.Upload;

                // Output
                // JUST before the whole process, get the timestamp, to be sure to 
                // get rows inserted / updated elsewhere since the sync is not over
                long clientTimestamp;
                (context, clientTimestamp) = await this.InternalGetLocalTimestampAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                DatabaseChangesSelected clientChangesSelected;

                // Locally, if we are new, no need to get changes
                if (isNew)
                    clientChangesSelected = new DatabaseChangesSelected();
                else
                    (context, clientChangesSelected) = await this.InternalGetEstimatedChangesCountAsync(
                        cScopeInfo, context,
                        isNew, lastTimestamp, clientTimestamp, remoteScopeId, this.Provider.SupportsMultipleActiveResultSets,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                var changes = new ClientSyncChanges(clientTimestamp, null, clientChangesSelected, null, null);

                return changes;

            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Get changes from local database from a specific scope you already fetched from local database
        /// </summary>
        internal virtual async Task<(SyncContext context, ClientSyncChanges syncChanges)>
            InternalGetChangesAsync(ScopeInfo cScopeInfo, SyncContext context, ScopeInfoClient cScopeInfoClient, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ChangesSelecting, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                // Output
                long clientTimestamp = 0L;
                BatchInfo clientBatchInfo = null;
                DatabaseChangesSelected clientChangesSelected = null;

                // If no schema in the client scope. Maybe the client scope table does not exists, or we never get the schema from server
                if (cScopeInfo.Schema == null)
                    throw new MissingLocalOrchestratorSchemaException();

                // On local, we don't want to chase rows from "others" 
                // We just want our local rows, so we dont exclude any remote scope id, by setting scope id to NULL
                Guid? remoteScopeId = null;

                //Direction set to Upload
                context.SyncWay = SyncWay.Upload;

                // JUST before the whole process, get the timestamp, to be sure to 
                // get rows inserted / updated elsewhere since the sync is not over
                (context, clientTimestamp) = await this.InternalGetLocalTimestampAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // Locally, if we are new, no need to get changes
                if (cScopeInfoClient.IsNewScope)
                {
                    // Create the batch info, in memory
                    string info = connection != null && !string.IsNullOrEmpty(connection.Database) ? $"{connection.Database}_EMPTYGETCHANGES" : "EMPTYGETCHANGES";
                    clientBatchInfo = new BatchInfo(this.Options.BatchDirectory, info: info);

                    // Create a new empty in-memory batch info
                    clientChangesSelected = new DatabaseChangesSelected();

                }
                else
                {
                    (context, clientBatchInfo, clientChangesSelected) = await this.InternalGetChangesAsync(cScopeInfo,
                        context, cScopeInfoClient.IsNewScope, cScopeInfoClient.LastSyncTimestamp, clientTimestamp, remoteScopeId, this.Provider.SupportsMultipleActiveResultSets,
                        this.Options.BatchDirectory, null, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
                }

                await runner.CommitAsync().ConfigureAwait(false);

                var changes = new ClientSyncChanges(clientTimestamp, clientBatchInfo, clientChangesSelected, null, null);

                await runner.CommitAsync().ConfigureAwait(false);

                return (context, changes);

            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }
    }
}
