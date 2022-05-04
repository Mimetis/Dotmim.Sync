using Dotmim.Sync.Args;
using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading;
using System.Threading.Tasks;


namespace Dotmim.Sync
{
    public partial class RemoteOrchestrator : BaseOrchestrator
    {

        /// <summary>
        /// Get a snapshot
        /// </summary>
        public virtual async Task<(long RemoteClientTimestamp, BatchInfo ServerBatchInfo, DatabaseChangesSelected DatabaseChangesSelected)>
            GetSnapshotAsync(IScopeInfo scopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            try
            {
                // Get context or create a new one
                var ctx = this.GetContext(scopeInfo.Name);
                var changesSelected = new DatabaseChangesSelected();

                BatchInfo serverBatchInfo = null;
                if (string.IsNullOrEmpty(this.Options.SnapshotsDirectory))
                    return (0, null, changesSelected);

                //Direction set to Download
                ctx.SyncWay = SyncWay.Download;

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Get Schema from remote provider if no schema passed from args
                if (scopeInfo.Schema == null)
                {
                    scopeInfo = await this.GetServerScopeInfoAsync(scopeInfo.Name, scopeInfo.Setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                // When we get the changes from server, we create the batches if it's requested by the client
                // the batch decision comes from batchsize from client
                var (rootDirectory, nameDirectory) = await this.InternalGetSnapshotDirectoryAsync(scopeInfo.Name, default, cancellationToken, progress).ConfigureAwait(false);

                if (!string.IsNullOrEmpty(rootDirectory))
                {
                    var directoryFullPath = Path.Combine(rootDirectory, nameDirectory);

                    // if no snapshot present, just return null value.
                    if (Directory.Exists(directoryFullPath))
                    {
                        // Serialize on disk.
                        var jsonConverter = new Serialization.JsonConverter<BatchInfo>();

                        var summaryFileName = Path.Combine(directoryFullPath, "summary.json");

                        using (var fs = new FileStream(summaryFileName, FileMode.Open, FileAccess.Read))
                        {
                            serverBatchInfo = await jsonConverter.DeserializeAsync(fs).ConfigureAwait(false);
                        }

                        // Create the schema changeset
                        var changesSet = new SyncSet();

                        // Create a Schema set without readonly columns, attached to memory changes
                        foreach (var table in scopeInfo.Schema.Tables)
                        {
                            DbSyncAdapter.CreateChangesTable(scopeInfo.Schema.Tables[table.TableName, table.SchemaName], changesSet);

                            // Get all stats about this table
                            var bptis = serverBatchInfo.BatchPartsInfo.SelectMany(bpi => bpi.Tables.Where(t =>
                            {
                                var sc = SyncGlobalization.DataSourceStringComparison;

                                var sn = t.SchemaName == null ? string.Empty : t.SchemaName;
                                var otherSn = table.SchemaName == null ? string.Empty : table.SchemaName;

                                return table.TableName.Equals(t.TableName, sc) && sn.Equals(otherSn, sc);

                            }));

                            if (bptis != null)
                            {
                                // Statistics
                                var tableChangesSelected = new TableChangesSelected(table.TableName, table.SchemaName)
                                {
                                    // we are applying a snapshot where it can't have any deletes, obviously
                                    Upserts = bptis.Sum(bpti => bpti.RowsCount)
                                };

                                if (tableChangesSelected.Upserts > 0)
                                    changesSelected.TableChangesSelected.Add(tableChangesSelected);
                            }


                        }
                        serverBatchInfo.SanitizedSchema = changesSet;
                    }
                }
                if (serverBatchInfo == null)
                    return (0, null, changesSelected);

                return (serverBatchInfo.Timestamp, serverBatchInfo, changesSelected);
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeInfo.Name, ex);
            }
        }




        public virtual Task<BatchInfo> CreateSnapshotAsync(SyncSetup setup = null, SyncParameters syncParameters = null,
            ILocalSerializerFactory localSerializerFactory = default, DbConnection connection = default, DbTransaction transaction = default,
            CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => CreateSnapshotAsync(SyncOptions.DefaultScopeName, setup, syncParameters, localSerializerFactory, connection, transaction, cancellationToken, progress);

        /// <summary>
        /// Create a snapshot, based on the Setup object. 
        /// </summary>
        /// <param name="syncParameters">if not parameters are found in the SyncContext instance, will use thes sync parameters instead</param>
        /// <returns>Instance containing all information regarding the snapshot</returns>
        public virtual async Task<BatchInfo> CreateSnapshotAsync(string scopeName, SyncSetup setup = null, SyncParameters syncParameters = null,
            ILocalSerializerFactory localSerializerFactory = default, DbConnection connection = default, DbTransaction transaction = default,
            CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(scopeName, SyncMode.Writing, SyncStage.SnapshotCreating, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                if (string.IsNullOrEmpty(this.Options.SnapshotsDirectory) || this.Options.BatchSize <= 0)
                    throw new SnapshotMissingMandatariesOptionsException();

                var context = this.GetContext(scopeName);

                // check parameters
                // If context has no parameters specified, and user specifies a parameter collection we switch them
                if ((context.Parameters == null || context.Parameters.Count <= 0) && syncParameters != null && syncParameters.Count > 0)
                    context.Parameters = syncParameters;

                // 1) Get Schema from remote provider
                var serverScopeInfo = await this.InternalGetServerScopeInfoAsync(scopeName, setup, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // If we just have create the server scope, we need to provision it
                if (serverScopeInfo != null && serverScopeInfo.IsNewScope)
                {
                    // 2) Provision
                    var provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

                    await this.InternalProvisionAsync(serverScopeInfo, false, provision, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    // Write scopes locally
                    serverScopeInfo = await this.InternalSaveServerScopeInfoAsync(serverScopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                // 4) Getting the most accurate timestamp
                var remoteClientTimestamp = await this.InternalGetLocalTimestampAsync(serverScopeInfo.Name, 
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // 5) Create the snapshot with
                localSerializerFactory = localSerializerFactory == null ? new LocalJsonSerializerFactory() : localSerializerFactory;

                var batchInfo = await this.InternalCreateSnapshotAsync(serverScopeInfo, localSerializerFactory, remoteClientTimestamp,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return batchInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeName, ex);
            }

        }

        internal virtual async Task<BatchInfo> InternalCreateSnapshotAsync(ServerScopeInfo serverScopeInfo,
              ILocalSerializerFactory localSerializerFactory, long remoteClientTimestamp, DbConnection connection, DbTransaction transaction,
              CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            var context = this.GetContext(serverScopeInfo.Name);

            await this.InterceptAsync(new SnapshotCreatingArgs(context, serverScopeInfo.Schema, this.Options.SnapshotsDirectory, this.Options.BatchSize, remoteClientTimestamp, this.Provider.CreateConnection(), null), progress, cancellationToken).ConfigureAwait(false);

            if (!Directory.Exists(this.Options.SnapshotsDirectory))
                Directory.CreateDirectory(this.Options.SnapshotsDirectory);

            var (rootDirectory, nameDirectory) = await this.InternalGetSnapshotDirectoryAsync(serverScopeInfo.Name, context.Parameters, cancellationToken, progress).ConfigureAwait(false);

            // create local directory with scope inside
            if (!Directory.Exists(rootDirectory))
                Directory.CreateDirectory(rootDirectory);

            // Delete directory if already exists
            var directoryFullPath = Path.Combine(rootDirectory, nameDirectory);

            // Delete old version if exists
            if (Directory.Exists(directoryFullPath))
                Directory.Delete(directoryFullPath, true);

            BatchInfo serverBatchInfo;

            (_, serverBatchInfo, _) =
                    await this.InternalGetChangesAsync(serverScopeInfo, true, null, Guid.Empty, 
                    this.Provider.SupportsMultipleActiveResultSets,
                    rootDirectory, nameDirectory, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            // since we explicitely defined remote client timestamp to null, to get all rows, just reaffect here
            serverBatchInfo.Timestamp = remoteClientTimestamp;

            // Serialize on disk.
            var jsonConverter = new Serialization.JsonConverter<BatchInfo>();

            var summaryFileName = Path.Combine(directoryFullPath, "summary.json");

            using (var f = new FileStream(summaryFileName, FileMode.CreateNew, FileAccess.ReadWrite))
            {
                var bytes = await jsonConverter.SerializeAsync(serverBatchInfo).ConfigureAwait(false);
                f.Write(bytes, 0, bytes.Length);
            }

            await this.InterceptAsync(new SnapshotCreatedArgs(context, serverBatchInfo, this.Provider.CreateConnection(), null), progress, cancellationToken).ConfigureAwait(false);

            return serverBatchInfo;
        }


    }
}
