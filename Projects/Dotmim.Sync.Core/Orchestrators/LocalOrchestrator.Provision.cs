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
    public partial class LocalOrchestrator : BaseOrchestrator
    {
        /// <summary>
        /// Provision the local database based on the orchestrator setup, and the provision enumeration
        /// </summary>
        /// <param name="overwrite">Overwrite existing objects</param>
        public virtual Task<SyncSet> ProvisionAsync(SyncSet schema = null, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var provision = SyncProvision.ClientScope | SyncProvision.Table |
                            SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable;

            if (schema == null)
                schema = new SyncSet(this.Setup);

            return this.ProvisionAsync(schema, provision, overwrite, null, connection, transaction, cancellationToken, progress);
        }

        /// <summary>
        /// Provision the local database based on the orchestrator setup, and the provision enumeration
        /// </summary>
        /// <param name="provision">Provision enumeration to determine which components to apply</param>
        public virtual Task<SyncSet> ProvisionAsync(SyncProvision provision, bool overwrite = false, ScopeInfo clientScopeInfo = null, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => this.ProvisionAsync(new SyncSet(this.Setup), provision, overwrite, clientScopeInfo, connection, transaction, cancellationToken, progress);


        /// <summary>
        /// Provision the local database based on the schema parameter, and the provision enumeration
        /// </summary>
        /// <param name="schema">Schema to be applied to the database managed by the orchestrator, through the provider.</param>
        /// <param name="provision">Provision enumeration to determine which components to apply</param>
        /// <param name="clientScopeInfo">client scope. Will be saved once provision is done</param>
        /// <returns>Full schema with table and columns properties</returns>
        public virtual async Task<SyncSet> ProvisionAsync(SyncSet schema, SyncProvision provision, bool overwrite = false, ScopeInfo clientScopeInfo = null, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Check incompatibility with the flags
                if (provision.HasFlag(SyncProvision.ServerHistoryScope) || provision.HasFlag(SyncProvision.ServerScope))
                    throw new InvalidProvisionForLocalOrchestratorException();

                // Get server scope if not supplied
                if (clientScopeInfo == null)
                {
                    var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

                    var exists = await this.InternalExistsScopeInfoTableAsync(this.GetContext(), DbScopeType.Client, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (exists)
                        clientScopeInfo = await this.InternalGetScopeAsync<ScopeInfo>(this.GetContext(), DbScopeType.Client, this.ScopeName, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                schema = await InternalProvisionAsync(this.GetContext(), overwrite, schema, this.Setup, provision, clientScopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return schema;
            }
            catch (Exception ex)
            {
                throw GetSyncError(ex);
            }
        }


        /// <summary>
        /// Deprovision the local database
        /// </summary>
        public virtual Task DeprovisionAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var provision = SyncProvision.ClientScope | SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable;

            return this.DeprovisionAsync(provision, null, connection, transaction, cancellationToken, progress);
        }


        /// <summary>
        /// Deprovision the orchestrator database based on the provision enumeration
        /// </summary>
        /// <param name="provision">Provision enumeration to determine which components to deprovision</param>
        public virtual async Task<bool> DeprovisionAsync(SyncProvision provision, ScopeInfo clientScopeInfo = null, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            // Create a temporary SyncSet for attaching to the schemaTable
            var tmpSchema = new SyncSet();

            // Add this table to schema
            foreach (var table in this.Setup.Tables)
                tmpSchema.Tables.Add(new SyncTable(table.TableName, table.SchemaName));

            tmpSchema.EnsureSchema();

            // copy filters from old setup
            foreach (var filter in this.Setup.Filters)
                tmpSchema.Filters.Add(filter);

            var isDeprovisioned = await this.DeprovisionAsync(tmpSchema, provision, clientScopeInfo, connection, transaction, cancellationToken, progress);

            return isDeprovisioned;

        }

        /// <summary>
        /// Deprovision the orchestrator database based on the schema argument, and the provision enumeration
        /// </summary>
        /// <param name="schema">Schema to be deprovisioned from the database managed by the orchestrator, through the provider.</param>
        /// <param name="provision">Provision enumeration to determine which components to deprovision</param>
        public virtual async Task<bool> DeprovisionAsync(SyncSet schema, SyncProvision provision, ScopeInfo clientScopeInfo = null, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(SyncStage.Deprovisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                // Get server scope if not supplied
                if (clientScopeInfo == null)
                {
                    var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

                    var exists = await this.InternalExistsScopeInfoTableAsync(this.GetContext(), DbScopeType.Client, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (exists)
                        clientScopeInfo = await this.InternalGetScopeAsync<ScopeInfo>(this.GetContext(), DbScopeType.Client, this.ScopeName, scopeBuilder, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                var isDeprovisioned = await InternalDeprovisionAsync(this.GetContext(), schema, this.Setup, provision, clientScopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

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
