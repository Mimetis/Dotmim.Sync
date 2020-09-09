using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
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
    /// <summary>
    /// Core provider : should be implemented by any server / client provider
    /// </summary>
    public abstract partial class CoreProvider
    {
        /// <summary>
        /// Deprovision a database. You have to passe a configuration object, containing at least the dmTables
        /// </summary>
        public async Task<SyncContext> DeprovisionAsync(SyncContext context, SyncSet schema, SyncSetup setup, SyncProvision provision, string scopeInfoTableName,
                             bool disableConstraintsOnApplyChanges, DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            if (schema.Tables == null || !schema.HasTables)
                throw new MissingTablesException();

            this.Orchestrator.logger.LogDebug(SyncEventsId.Deprovision, new { TablesCount = schema.Tables.Count, ScopeInfoTableName = scopeInfoTableName, DisableConstraintsOnApplyChanges = disableConstraintsOnApplyChanges, });

            // get Database builder
            var builder = this.GetDatabaseBuilder();
            builder.UseChangeTracking = this.UseChangeTracking;
            builder.UseBulkProcedures = this.SupportBulkOperations;


            // Sorting tables based on dependencies between them
            var schemaTables = schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));


            // Disable check constraints
            if (disableConstraintsOnApplyChanges)
                foreach (var table in schemaTables.Reverse())
                    await this.DisableConstraintsAsync(context, table, setup, connection, transaction).ConfigureAwait(false);

            // Creating a local function to mutualize call
            var deprovisionFuncAsync = new Func<SyncProvision, IEnumerable<SyncTable>, Task>(async (p, tables) =>
            {
                foreach (var schemaTable in tables)
                {
                    var tableBuilder = this.GetTableBuilder(schemaTable, setup);
                    // set if the builder supports creating the bulk operations proc stock
                    tableBuilder.UseBulkProcedures = this.SupportBulkOperations;
                    tableBuilder.UseChangeTracking = this.UseChangeTracking;

                    // adding filter
                    this.AddFilters(schemaTable, tableBuilder);

                    this.Orchestrator.logger.LogDebug(SyncEventsId.Deprovision, schemaTable);

                    await tableBuilder.DropAsync(p, connection, transaction).ConfigureAwait(false);

                    // Interceptor
                    await this.Orchestrator.InterceptAsync(new TableDeprovisionedArgs(context, p, schemaTable, connection, transaction), cancellationToken).ConfigureAwait(false);
                }
            });

            // Checking if we have to deprovision tables
            bool hasDeprovisionTableFlag = provision.HasFlag(SyncProvision.Table);

            // Firstly, removing the flag from the provision, because we need to drop everything in correct order, then drop tables in reverse side
            if (hasDeprovisionTableFlag)
                provision ^= SyncProvision.Table;

            // Deprovision everything in order, excepting table
            await deprovisionFuncAsync(provision, schemaTables).ConfigureAwait(false);

            // then in reverse side, deprovision tables, if Table was part of Provision enumeration.
            if (hasDeprovisionTableFlag)
                await deprovisionFuncAsync(SyncProvision.Table, schemaTables.Reverse()).ConfigureAwait(false);


            if (provision.HasFlag(SyncProvision.ClientScope))
                context = await this.DropClientScopeAsync(context, scopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (provision.HasFlag(SyncProvision.ServerScope))
                context = await this.DropServerScopeAsync(context, scopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (provision.HasFlag(SyncProvision.ServerHistoryScope))
                context = await this.DropServerHistoryScopeAsync(context, scopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            return context;
        }


        /// <summary>
        /// Be sure all tables are ready and configured for sync
        /// the ScopeSet Configuration MUST be filled by the schema form Database
        /// </summary>
        public virtual async Task<SyncContext> ProvisionAsync(SyncContext context, SyncSet schema, SyncSetup setup, SyncProvision provision, string scopeInfoTableName,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            if (schema.Tables == null || !schema.HasTables)
                throw new MissingTablesException();

            this.Orchestrator.logger.LogDebug(SyncEventsId.Provision, new { TablesCount = schema.Tables.Count, ScopeInfoTableName = scopeInfoTableName });

            // get Database builder
            var builder = this.GetDatabaseBuilder();
            builder.UseChangeTracking = this.UseChangeTracking;
            builder.UseBulkProcedures = this.SupportBulkOperations;

            // Initialize database if needed
            await builder.EnsureDatabaseAsync(connection, transaction).ConfigureAwait(false);

            // Shoudl we create scope
            if (provision.HasFlag(SyncProvision.ClientScope))
                context = await this.EnsureClientScopeAsync(context, scopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (provision.HasFlag(SyncProvision.ServerScope))
                context = await this.EnsureServerScopeAsync(context, scopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (provision.HasFlag(SyncProvision.ServerHistoryScope))
                context = await this.EnsureServerHistoryScopeAsync(context, scopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);


            // Sorting tables based on dependencies between them
            var schemaTables = schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));
            

            foreach (var schemaTable in schemaTables)
            {
                var tableBuilder = this.GetTableBuilder(schemaTable, setup);
                // set if the builder supports creating the bulk operations proc stock
                tableBuilder.UseBulkProcedures = this.SupportBulkOperations;
                tableBuilder.UseChangeTracking = this.UseChangeTracking;

                // adding filter
                this.AddFilters(schemaTable, tableBuilder);

                this.Orchestrator.logger.LogDebug(SyncEventsId.Provision, schemaTable);

                // Interceptor
                await this.Orchestrator.InterceptAsync(new TableProvisioningArgs(context, provision, tableBuilder, connection, transaction), cancellationToken).ConfigureAwait(false);

                await tableBuilder.CreateAsync(provision, connection, transaction).ConfigureAwait(false);

                // Interceptor
                await this.Orchestrator.InterceptAsync(new TableProvisionedArgs(context, provision, schemaTable, connection, transaction), cancellationToken).ConfigureAwait(false);
            }


            return context;
        }

        /// <summary>
        /// Adding filters to an existing configuration
        /// </summary>
        private void AddFilters(SyncTable schemaTable, DbTableBuilder builder)
        {
            var schema = schemaTable.Schema;

            if (schema.Filters != null && schema.Filters.Count > 0)
            {
                // get the all the filters for the table
                builder.Filter = schemaTable.GetFilter();

                this.Orchestrator.logger.LogDebug(SyncEventsId.AddFilter, builder.Filter);
            }
        }


        public virtual async Task<SyncContext> UpdateUntrackedRowsAsync(SyncContext context, SyncSet schema, SyncSetup setup,
                                                 DbConnection connection, DbTransaction transaction,
                                                 CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            if (schema.Tables == null || !schema.HasTables)
                throw new MissingTablesException();

            //this.Orchestrator.logger.LogDebug(SyncEventsId.Provision, new { TablesCount = schema.Tables.Count, ScopeInfoTableName = scopeInfoTableName });

            foreach (var syncTable in schema.Tables)
            {
                var tableBuilder = this.GetTableBuilder(syncTable, setup);
                var syncAdapter = tableBuilder.CreateSyncAdapter();

                await syncAdapter.UpdateUntrackedRowsAsync(connection, transaction).ConfigureAwait(false);
            }

            return context;
        }
    }
}
