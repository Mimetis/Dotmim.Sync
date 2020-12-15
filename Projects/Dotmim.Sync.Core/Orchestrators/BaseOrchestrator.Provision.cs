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
        public virtual Task<SyncSet> ProvisionAsync(SyncProvision provision, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => this.ProvisionAsync(new SyncSet(this.Setup), provision, cancellationToken, progress);

        /// <summary>
        /// Provision the orchestrator database based on the schema argument, and the provision enumeration
        /// </summary>
        /// <param name="schema">Schema to be applied to the database managed by the orchestrator, through the provider.</param>
        /// <param name="provision">Provision enumeration to determine which components to apply</param>
        /// <returns>Full schema with table and columns properties</returns>
        public virtual Task<SyncSet> ProvisionAsync(SyncSet schema, SyncProvision provision, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => RunInTransactionAsync(new Func<SyncContext, DbConnection, DbTransaction, Task<SyncSet>>(async (ctx, connection, transaction) =>
            {
                // Check incompatibility with the flags
                if (this is LocalOrchestrator && (provision.HasFlag(SyncProvision.ServerHistoryScope) || provision.HasFlag(SyncProvision.ServerScope)))
                    throw new InvalidProvisionForLocalOrchestratorException();
                else if (!(this is LocalOrchestrator) && provision.HasFlag(SyncProvision.ClientScope))
                    throw new InvalidProvisionForRemoteOrchestratorException();

                await this.InterceptAsync(new DatabaseProvisioningArgs(ctx, provision, schema, connection, transaction), cancellationToken).ConfigureAwait(false);

                ctx.SyncStage = SyncStage.Provisioning;

                ctx = await InternalProvisionAsync(ctx, schema, provision, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                ctx.SyncStage = SyncStage.Provisioned;

                var args = new DatabaseProvisionedArgs(ctx, provision, schema, connection);
                await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);
                this.ReportProgress(ctx, progress, args);

                return schema;

            }), cancellationToken);


        internal async Task<SyncContext> InternalProvisionAsync(SyncContext ctx, SyncSet schema, SyncProvision provision, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            this.logger.LogDebug(SyncEventsId.Provision, new { TablesCount = schema.Tables.Count, ScopeInfoTableName = this.Options.ScopeInfoTableName });

            // get Database builder
            var builder = this.Provider.GetDatabaseBuilder();

            // Initialize database if needed
            await builder.EnsureDatabaseAsync(connection, transaction).ConfigureAwait(false);

            // Get Scope Builder
            var scopeBuilder = this.Provider.GetScopeBuilder(this.Options.ScopeInfoTableName);

            // Shoudl we create scope
            if (provision.HasFlag(SyncProvision.ClientScope))
            {
                var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.Client, scopeBuilder, connection, transaction, cancellationToken);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(ctx, DbScopeType.Client, scopeBuilder, connection, transaction, cancellationToken);
            }

            if (provision.HasFlag(SyncProvision.ServerScope))
            {
                var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.Server, scopeBuilder, connection, transaction, cancellationToken);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(ctx, DbScopeType.Server, scopeBuilder, connection, transaction, cancellationToken);
            }

            if (provision.HasFlag(SyncProvision.ServerHistoryScope))
            {
                var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.ServerHistory, scopeBuilder, connection, transaction, cancellationToken);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(ctx, DbScopeType.ServerHistory, scopeBuilder, connection, transaction, cancellationToken);
            }

            // Sorting tables based on dependencies between them
            var schemaTables = schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            foreach (var schemaTable in schemaTables)
            {
                var tableBuilder = this.Provider.GetTableBuilder(schemaTable, this.Setup);

                this.logger.LogDebug(SyncEventsId.Provision, schemaTable);

                // Interceptor
                await this.InterceptAsync(new TableProvisioningArgs(ctx, provision, tableBuilder, connection, transaction), cancellationToken).ConfigureAwait(false);

                if (provision.HasFlag(SyncProvision.Table))
                {
                    var tableExistst = await this.InternalExistsTableAsync(ctx, schemaTable, tableBuilder, connection, transaction, cancellationToken).ConfigureAwait(false);

                    if (!tableExistst)
                        await this.InternalCreateTableAsync(ctx, schemaTable, tableBuilder, connection, transaction, cancellationToken).ConfigureAwait(false);
                }

                if (provision.HasFlag(SyncProvision.TrackingTable))
                {
                    var trackingTableExistst = await this.InternalExistsTrackingTableAsync(ctx, schemaTable, tableBuilder, connection, transaction, cancellationToken).ConfigureAwait(false);

                    if (!trackingTableExistst)
                        await this.InternalCreateTrackingTableAsync(ctx, schemaTable, tableBuilder, connection, transaction, cancellationToken).ConfigureAwait(false);
                }

                if (provision.HasFlag(SyncProvision.Triggers))
                {
                    foreach (DbTriggerType triggerType in Enum.GetValues(typeof(DbTriggerType)))
                    {
                        var exists = await InternalExistsTriggerAsync(ctx, schemaTable, tableBuilder, triggerType, connection, transaction, cancellationToken);

                        // Drop trigger if already exists
                        if (exists)
                            await InternalDropTriggerAsync(ctx, schemaTable, tableBuilder, triggerType, connection, transaction, cancellationToken).ConfigureAwait(false);

                        await InternalCreateTriggerAsync(ctx, schemaTable, tableBuilder, triggerType, connection, transaction, cancellationToken).ConfigureAwait(false);
                    }
                }

                if (provision.HasFlag(SyncProvision.StoredProcedures))
                {
                    // First , delete all the stored procedures, except the Bulk Type, since we can delete them before deleting the attached stored procedures
                    foreach (DbStoredProcedureType storedProcedureType in Enum.GetValues(typeof(DbStoredProcedureType)))
                    {
                        if (storedProcedureType is DbStoredProcedureType.BulkTableType)
                            continue;

                        var exists = await InternalExistsStoredProcedureAsync(ctx, schemaTable, tableBuilder, storedProcedureType, connection, transaction, cancellationToken);

                        if (exists)
                            await InternalDropStoredProcedureAsync(ctx, schemaTable, tableBuilder, storedProcedureType, connection, transaction, cancellationToken).ConfigureAwait(false);
                    }


                    // Then delete bulk type if exists and provider supports them
                    if (this.Provider.SupportBulkOperations)
                    {
                        var exists = await InternalExistsStoredProcedureAsync(ctx, schemaTable, tableBuilder, DbStoredProcedureType.BulkTableType, connection, transaction, cancellationToken);
                        if (exists)
                            await InternalDropStoredProcedureAsync(ctx, schemaTable, tableBuilder, DbStoredProcedureType.BulkTableType, connection, transaction, cancellationToken).ConfigureAwait(false);
                    }

                    // Then create everything again
                    foreach (DbStoredProcedureType storedProcedureType in Enum.GetValues(typeof(DbStoredProcedureType)))
                    {
                        // if we are iterating on bulk, but provider do not support it, just loop through and continue
                        if ((storedProcedureType is DbStoredProcedureType.BulkTableType || storedProcedureType is DbStoredProcedureType.BulkUpdateRows || storedProcedureType is DbStoredProcedureType.BulkDeleteRows)
                            && !this.Provider.SupportBulkOperations)
                            continue;

                        await InternalCreateStoredProcedureAsync(ctx, schemaTable, tableBuilder, storedProcedureType, connection, transaction, cancellationToken).ConfigureAwait(false);
                    }
                }

                // Interceptor
                await this.InterceptAsync(new TableProvisionedArgs(ctx, provision, schemaTable, connection, transaction), cancellationToken).ConfigureAwait(false);
            }

            return ctx;
        }


        /// <summary>
        /// Deprovision the orchestrator database based on the Setup table argument, and the provision enumeration
        /// </summary>
        /// <param name="provision">Provision enumeration to determine which components to deprovision</param>
        public virtual Task DeprovisionAsync(SetupTable table, SyncProvision provision, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var setup = new SyncSetup();
            setup.Tables.Add(table);

            return this.DeprovisionAsync(new SyncSet(setup), provision, cancellationToken, progress);
        }

        /// <summary>
        /// Deprovision the orchestrator database based on the orchestrator Setup instance, provided on constructor, and the provision enumeration
        /// </summary>
        /// <param name="provision">Provision enumeration to determine which components to deprovision</param>
        public virtual Task DeprovisionAsync(SyncProvision provision, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => this.DeprovisionAsync(new SyncSet(this.Setup), provision, cancellationToken, progress);

        /// <summary>
        /// Deprovision the orchestrator database based on the schema argument, and the provision enumeration
        /// </summary>
        /// <param name="schema">Schema to be deprovisioned from the database managed by the orchestrator, through the provider.</param>
        /// <param name="provision">Provision enumeration to determine which components to deprovision</param>
        public virtual Task DeprovisionAsync(SyncSet schema, SyncProvision provision, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => RunInTransactionAsync(new Func<SyncContext, DbConnection, DbTransaction, Task<bool>>(async (ctx, connection, transaction) =>
            {
                this.logger.LogInformation(SyncEventsId.Deprovision, new { connection.Database, Provision = provision });

                ctx.SyncStage = SyncStage.Deprovisioning;

                await this.InterceptAsync(new DatabaseDeprovisioningArgs(ctx, provision, schema, connection, transaction), cancellationToken).ConfigureAwait(false);

                // get Database builder
                var builder = this.Provider.GetDatabaseBuilder();

                // Sorting tables based on dependencies between them
                var schemaTables = schema.Tables
                    .SortByDependencies(tab => tab.GetRelations()
                        .Select(r => r.GetParentTable()));

                // Disable check constraints
                if (this.Options.DisableConstraintsOnApplyChanges)
                    foreach (var table in schemaTables.Reverse())
                        await this.DisableConstraintsAsync(ctx, table, this.Setup, connection, transaction).ConfigureAwait(false);


                // Checking if we have to deprovision tables
                bool hasDeprovisionTableFlag = provision.HasFlag(SyncProvision.Table);

                // Firstly, removing the flag from the provision, because we need to drop everything in correct order, then drop tables in reverse side
                if (hasDeprovisionTableFlag)
                    provision ^= SyncProvision.Table;

                foreach (var schemaTable in schemaTables)
                {
                    var tableBuilder = this.Provider.GetTableBuilder(schemaTable, this.Setup);

                    this.logger.LogDebug(SyncEventsId.Deprovision, schemaTable);

                    // Interceptor
                    // Todo
                    // await this.InterceptAsync(new TableProvisioningArgs(ctx, provision, tableBuilder, connection, transaction), cancellationToken).ConfigureAwait(false);

                    if (provision.HasFlag(SyncProvision.StoredProcedures))
                    {
                        foreach (DbStoredProcedureType storedProcedureType in Enum.GetValues(typeof(DbStoredProcedureType)))
                        {
                            // if we are iterating on bulk, but provider do not support it, just loop through and continue
                            if ((storedProcedureType is DbStoredProcedureType.BulkTableType || storedProcedureType is DbStoredProcedureType.BulkUpdateRows || storedProcedureType is DbStoredProcedureType.BulkDeleteRows)
                                && !this.Provider.SupportBulkOperations)
                                continue;

                            var exists = await InternalExistsStoredProcedureAsync(ctx, schemaTable, tableBuilder, storedProcedureType, connection, transaction, cancellationToken);

                            // Drop storedProcedure if already exists
                            if (exists)
                                await InternalDropStoredProcedureAsync(ctx, schemaTable, tableBuilder, storedProcedureType, connection, transaction, cancellationToken).ConfigureAwait(false);
                        }
                    }

                    if (provision.HasFlag(SyncProvision.Triggers))
                    {
                        foreach (DbTriggerType triggerType in Enum.GetValues(typeof(DbTriggerType)))
                        {
                            var exists = await InternalExistsTriggerAsync(ctx, schemaTable, tableBuilder, triggerType, connection, transaction, cancellationToken);

                            // Drop trigger if already exists
                            if (exists)
                                await InternalDropTriggerAsync(ctx, schemaTable, tableBuilder, triggerType, connection, transaction, cancellationToken).ConfigureAwait(false);
                        }
                    }

                    if (provision.HasFlag(SyncProvision.TrackingTable))
                    {
                        var exists = await InternalExistsTrackingTableAsync(ctx, schemaTable, tableBuilder, connection, transaction, cancellationToken);

                        if (exists)
                            await this.InternalDropTrackingTableAsync(ctx, schemaTable, tableBuilder, connection, transaction, cancellationToken).ConfigureAwait(false);
                    }

                    // Launch interceptor if we're done here
                    if (!hasDeprovisionTableFlag)
                        await this.InterceptAsync(new TableDeprovisionedArgs(ctx, provision, schemaTable, connection, transaction), cancellationToken).ConfigureAwait(false);

                }

                // Eventually if we have the "Table" flag, then drop the table
                if (hasDeprovisionTableFlag)
                {
                    foreach (var schemaTable in schemaTables.Reverse())
                    {
                        var tableBuilder = this.Provider.GetTableBuilder(schemaTable, this.Setup);

                        var exists = await InternalExistsTableAsync(ctx, schemaTable, tableBuilder, connection, transaction, cancellationToken);

                        if (exists)
                            await this.InternalDropTableAsync(ctx, schemaTable, tableBuilder, connection, transaction, cancellationToken).ConfigureAwait(false);

                        await this.InterceptAsync(new TableDeprovisionedArgs(ctx, provision, schemaTable, connection, transaction), cancellationToken).ConfigureAwait(false);
                    }
                }

                // Get Scope Builder
                var scopeBuilder = this.Provider.GetScopeBuilder(this.Options.ScopeInfoTableName);

                if (provision.HasFlag(SyncProvision.ClientScope))
                {
                    var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.Client, scopeBuilder, connection, transaction, cancellationToken);

                    if (exists)
                        await this.InternalDropScopeInfoTableAsync(ctx, DbScopeType.Client, scopeBuilder, connection, transaction, cancellationToken);
                }

                if (provision.HasFlag(SyncProvision.ServerScope))
                {
                    var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.Server, scopeBuilder, connection, transaction, cancellationToken);

                    if (exists)
                        await this.InternalDropScopeInfoTableAsync(ctx, DbScopeType.Server, scopeBuilder, connection, transaction, cancellationToken);
                }

                if (provision.HasFlag(SyncProvision.ServerHistoryScope))
                {
                    var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.ServerHistory, scopeBuilder, connection, transaction, cancellationToken);

                    if (exists)
                        await this.InternalDropScopeInfoTableAsync(ctx, DbScopeType.ServerHistory, scopeBuilder, connection, transaction, cancellationToken);
                }
                // --

                ctx.SyncStage = SyncStage.Deprovisioned;

                var args = new DatabaseDeprovisionedArgs(ctx, provision, schema, connection);
                await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);
                this.ReportProgress(ctx, progress, args);

                return true;
            }), cancellationToken);

    }
}
