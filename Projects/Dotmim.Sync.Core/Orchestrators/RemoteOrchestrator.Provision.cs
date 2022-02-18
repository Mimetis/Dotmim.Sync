using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
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
        /// Provision the remote database 
        /// </summary>
        /// <param name="overwrite">Overwrite existing objects</param>
        public virtual Task<SyncSet> ProvisionAsync(bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var provision = SyncProvision.ServerScope | SyncProvision.ServerHistoryScope |
                            SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable;

            return this.ProvisionAsync(provision, overwrite, null, connection, transaction, cancellationToken, progress);
        }

        /// <summary>
        /// Provision the remote database based on the Setup parameter, and the provision enumeration
        /// </summary>
        /// <param name="provision">Provision enumeration to determine which components to apply</param>
        /// <param name="serverScopeInfo">server scope. Will be saved once provision is done</param>
        /// <returns>Full schema with table and columns properties</returns>
        public virtual async Task<SyncSet> ProvisionAsync(SyncProvision provision, bool overwrite = false, ServerScopeInfo serverScopeInfo = null, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {

                await using var runner = await this.GetConnectionAsync(SyncMode.Writing, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                // Check incompatibility with the flags
                if (provision.HasFlag(SyncProvision.ClientScope))
                    throw new InvalidProvisionForRemoteOrchestratorException();

                var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

                if (serverScopeInfo == null)
                {
                    var exists = await this.InternalExistsScopeInfoTableAsync(this.GetContext(), DbScopeType.Server, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                    if (exists)
                        serverScopeInfo = await this.InternalGetScopeAsync<ServerScopeInfo>(this.GetContext(), DbScopeType.Server, this.ScopeName, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                }


                var schema = serverScopeInfo?.Schema != null ? serverScopeInfo?.Schema : new SyncSet(this.Setup);
                schema = await InternalProvisionAsync(this.GetContext(), overwrite, schema, this.Setup, provision, serverScopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);
                return schema;
            }
            catch (Exception ex)
            {
                throw GetSyncError(ex);
            }
        }

        /// <summary>
        /// Deprovision the remote database 
        /// </summary>
        /// <param name="overwrite">Overwrite existing objects</param>
        public virtual Task<bool> DeprovisionAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var provision = SyncProvision.ServerScope | SyncProvision.ServerHistoryScope |
                            SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable;

            return this.DeprovisionAsync(provision, null, connection, transaction, cancellationToken, progress);
        }

        /// <summary>
        /// Deprovision the orchestrator database based on the schema argument, and the provision enumeration
        /// </summary>
        /// <param name="schema">Schema to be deprovisioned from the database managed by the orchestrator, through the provider.</param>
        /// <param name="provision">Provision enumeration to determine which components to deprovision</param>
        public virtual async Task<bool> DeprovisionAsync(SyncProvision provision, ServerScopeInfo serverScopeInfo = null, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(SyncMode.Writing, SyncStage.Deprovisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                // Get server scope if not supplied
                if (serverScopeInfo == null)
                {
                    var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

                    var exists = await this.InternalExistsScopeInfoTableAsync(this.GetContext(), DbScopeType.Server, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (exists)
                        serverScopeInfo = await this.InternalGetScopeAsync<ServerScopeInfo>(this.GetContext(), DbScopeType.Server, this.ScopeName, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                // Create a temporary SyncSet for attaching to the schemaTable
                var tmpSchema = new SyncSet();

                // Add this table to schema
                foreach (var table in this.Setup.Tables)
                    tmpSchema.Tables.Add(new SyncTable(table.TableName, table.SchemaName));

                tmpSchema.EnsureSchema();

                // copy filters from old setup
                foreach (var filter in this.Setup.Filters)
                    tmpSchema.Filters.Add(filter);

                var isDeprovisioned = await InternalDeprovisionAsync(this.GetContext(), tmpSchema, this.Setup, provision, serverScopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return isDeprovisioned;
            }
            catch (Exception ex)
            {
                throw GetSyncError(ex);
            }

        }
    }
}
