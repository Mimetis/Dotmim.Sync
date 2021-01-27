using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class BaseOrchestrator
    {

        /// <summary>
        /// Provision the orchestrator database based on the orchestrator Setup, and the provision enumeration
        /// </summary>
        /// <param name="provision">Provision enumeration to determine which components to apply</param>
        public virtual Task<SyncSet> ProvisionAsync(SyncProvision provision, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => this.ProvisionAsync(new SyncSet(this.Setup), provision, overwrite, connection, transaction, cancellationToken, progress);

        /// <summary>
        /// Provision the orchestrator database based on the schema argument, and the provision enumeration
        /// </summary>
        /// <param name="schema">Schema to be applied to the database managed by the orchestrator, through the provider.</param>
        /// <param name="provision">Provision enumeration to determine which components to apply</param>
        /// <returns>Full schema with table and columns properties</returns>
        public virtual Task<SyncSet> ProvisionAsync(SyncSet schema, SyncProvision provision, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => RunInTransactionAsync(SyncStage.Provisioning, async (ctx, connection, transaction) =>
            {
                // Check incompatibility with the flags
                if (this is LocalOrchestrator && (provision.HasFlag(SyncProvision.ServerHistoryScope) || provision.HasFlag(SyncProvision.ServerScope)))
                    throw new InvalidProvisionForLocalOrchestratorException();
                else if (!(this is LocalOrchestrator) && provision.HasFlag(SyncProvision.ClientScope))
                    throw new InvalidProvisionForRemoteOrchestratorException();

                schema = await InternalProvisionAsync(ctx, overwrite, schema, provision, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                return schema;

            }, connection, transaction, cancellationToken);

        /// <summary>
        /// Deprovision the orchestrator database based on the Setup table argument, and the provision enumeration
        /// </summary>
        /// <param name="provision">Provision enumeration to determine which components to deprovision</param>
        public virtual Task DeprovisionAsync(SetupTable table, SyncProvision provision, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var setup = new SyncSetup();
            setup.Tables.Add(table);

            // using a fake SyncTable based on oldSetup, since we don't need columns, but we need to have the filters
            var schemaTable = new SyncTable(table.TableName, table.SchemaName);

            // Create a temporary SyncSet for attaching to the schemaTable
            var tmpSchema = new SyncSet();

            // Add this table to schema
            tmpSchema.Tables.Add(schemaTable);

            tmpSchema.EnsureSchema();

            // copy filters from old setup
            foreach (var filter in this.Setup.Filters)
                tmpSchema.Filters.Add(filter);


            return this.DeprovisionAsync(tmpSchema, provision, connection, transaction, cancellationToken, progress);
        }

        /// <summary>
        /// Deprovision the orchestrator database based on the orchestrator Setup instance, provided on constructor, and the provision enumeration
        /// </summary>
        /// <param name="provision">Provision enumeration to determine which components to deprovision</param>
        public virtual Task DeprovisionAsync(SyncProvision provision, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
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

            return this.DeprovisionAsync(tmpSchema, provision, connection, transaction, cancellationToken, progress);
        }

        /// <summary>
        /// Deprovision the orchestrator database based on the schema argument, and the provision enumeration
        /// </summary>
        /// <param name="schema">Schema to be deprovisioned from the database managed by the orchestrator, through the provider.</param>
        /// <param name="provision">Provision enumeration to determine which components to deprovision</param>
        public virtual Task DeprovisionAsync(SyncSet schema, SyncProvision provision, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.Deprovisioning, (ctx, connection, transaction) =>
        {
            return InternalDeprovisionAsync(ctx, schema, provision, connection, transaction, cancellationToken, progress);

        }, connection, transaction, cancellationToken);

        internal async Task<SyncSet> InternalProvisionAsync(SyncContext ctx, bool overwrite, SyncSet schema, SyncProvision provision, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // If schema does not have any table, raise an exception
            if (schema == null || schema.Tables == null || !schema.HasTables)
                throw new MissingTablesException();

            await this.InterceptAsync(new ProvisioningArgs(ctx, provision, schema, connection, transaction), cancellationToken).ConfigureAwait(false);

            // get Database builder
            var builder = this.Provider.GetDatabaseBuilder();

            // Initialize database if needed
            await builder.EnsureDatabaseAsync(connection, transaction).ConfigureAwait(false);

            // Check if we have tables AND columns
            // If we don't have any columns it's most probably because user called method with the Setup only
            // So far we have only tables names, it's enough to get the schema
            if (schema.HasTables && !schema.HasColumns)
                schema = await this.InternalGetSchemaAsync(ctx, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            // Get Scope Builder
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            // Shoudl we create scope
            if (provision.HasFlag(SyncProvision.ClientScope))
            {
                var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.Client, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(ctx, DbScopeType.Client, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            }

            if (provision.HasFlag(SyncProvision.ServerScope))
            {
                var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.Server, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(ctx, DbScopeType.Server, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            }

            if (provision.HasFlag(SyncProvision.ServerHistoryScope))
            {
                var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.ServerHistory, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(ctx, DbScopeType.ServerHistory, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            }

            // Sorting tables based on dependencies between them
            var schemaTables = schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            foreach (var schemaTable in schemaTables)
            {
                var tableBuilder = this.GetTableBuilder(schemaTable, this.Setup);

                // Check if we need to create a schema there
                var schemaExists = await InternalExistsSchemaAsync(ctx, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!schemaExists)
                    await InternalCreateSchemaAsync(ctx, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (provision.HasFlag(SyncProvision.Table))
                {
                    var tableExistst = await this.InternalExistsTableAsync(ctx, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (!tableExistst)
                        await this.InternalCreateTableAsync(ctx, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                if (provision.HasFlag(SyncProvision.TrackingTable))
                {
                    var trackingTableExistst = await this.InternalExistsTrackingTableAsync(ctx, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (!trackingTableExistst)
                        await this.InternalCreateTrackingTableAsync(ctx, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                if (provision.HasFlag(SyncProvision.Triggers))
                    await this.InternalCreateTriggersAsync(ctx, overwrite, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                
                if (provision.HasFlag(SyncProvision.StoredProcedures))
                    await this.InternalCreateStoredProceduresAsync(ctx, overwrite, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            }

            var args = new ProvisionedArgs(ctx, provision, schema, connection);
            await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);
            this.ReportProgress(ctx, progress, args);

            return schema;
        }

        internal async Task<bool> InternalDeprovisionAsync(SyncContext ctx, SyncSet schema, SyncProvision provision, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            await this.InterceptAsync(new DeprovisioningArgs(ctx, provision, schema, connection, transaction), cancellationToken).ConfigureAwait(false);

            // get Database builder
            var builder = this.Provider.GetDatabaseBuilder();

            // Sorting tables based on dependencies between them
            var schemaTables = schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            // Disable check constraints
            if (this.Options.DisableConstraintsOnApplyChanges)
                foreach (var table in schemaTables.Reverse())
                    await this.InternalDisableConstraintsAsync(ctx, this.GetSyncAdapter(table, this.Setup), connection, transaction).ConfigureAwait(false);


            // Checking if we have to deprovision tables
            bool hasDeprovisionTableFlag = provision.HasFlag(SyncProvision.Table);

            // Firstly, removing the flag from the provision, because we need to drop everything in correct order, then drop tables in reverse side
            if (hasDeprovisionTableFlag)
                provision ^= SyncProvision.Table;

            foreach (var schemaTable in schemaTables)
            {
                var tableBuilder = this.GetTableBuilder(schemaTable, this.Setup);

                if (provision.HasFlag(SyncProvision.StoredProcedures))
                {
                    var allStoredProcedures = new List<DbStoredProcedureType>();

                    foreach (var spt in Enum.GetValues(typeof(DbStoredProcedureType)))
                        allStoredProcedures.Add((DbStoredProcedureType)spt);

                    allStoredProcedures.Reverse();

                    foreach (DbStoredProcedureType storedProcedureType in allStoredProcedures)
                    {
                        // if we are iterating on bulk, but provider do not support it, just loop through and continue
                        if ((storedProcedureType is DbStoredProcedureType.BulkTableType || storedProcedureType is DbStoredProcedureType.BulkUpdateRows || storedProcedureType is DbStoredProcedureType.BulkDeleteRows)
                            && !this.Provider.SupportBulkOperations)
                            continue;

                        var exists = await InternalExistsStoredProcedureAsync(ctx, tableBuilder, storedProcedureType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Drop storedProcedure if already exists
                        if (exists)
                            await InternalDropStoredProcedureAsync(ctx, tableBuilder, storedProcedureType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    }

                    // Removing cached commands
                    var syncAdapter = this.GetSyncAdapter(schemaTable, this.Setup);
                    syncAdapter.RemoveCommands();
                }

                if (provision.HasFlag(SyncProvision.Triggers))
                {
                    foreach (DbTriggerType triggerType in Enum.GetValues(typeof(DbTriggerType)))
                    {
                        var exists = await InternalExistsTriggerAsync(ctx, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Drop trigger if already exists
                        if (exists)
                            await InternalDropTriggerAsync(ctx, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    }
                }

                if (provision.HasFlag(SyncProvision.TrackingTable))
                {
                    var exists = await InternalExistsTrackingTableAsync(ctx, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (exists)
                        await this.InternalDropTrackingTableAsync(ctx, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                }
            }

            // Eventually if we have the "Table" flag, then drop the table
            if (hasDeprovisionTableFlag)
            {
                foreach (var schemaTable in schemaTables.Reverse())
                {
                    var tableBuilder = this.GetTableBuilder(schemaTable, this.Setup);

                    var exists = await InternalExistsTableAsync(ctx, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (exists)
                        await this.InternalDropTableAsync(ctx, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                }
            }

            // Get Scope Builder
            var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            if (provision.HasFlag(SyncProvision.ClientScope))
            {
                var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.Client, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (exists)
                    await this.InternalDropScopeInfoTableAsync(ctx, DbScopeType.Client, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            }

            if (provision.HasFlag(SyncProvision.ServerScope))
            {
                var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.Server, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (exists)
                    await this.InternalDropScopeInfoTableAsync(ctx, DbScopeType.Server, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            }

            if (provision.HasFlag(SyncProvision.ServerHistoryScope))
            {
                var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.ServerHistory, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (exists)
                    await this.InternalDropScopeInfoTableAsync(ctx, DbScopeType.ServerHistory, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            }

            var args = new DeprovisionedArgs(ctx, provision, schema, connection);
            await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);
            this.ReportProgress(ctx, progress, args);

            return true;
        }

    }
}
