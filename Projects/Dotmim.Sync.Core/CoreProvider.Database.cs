using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
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
        public async Task<SyncContext> DeprovisionAsync(SyncContext context, SyncSet schema, SyncProvision provision, string scopeInfoTableName,
                             bool disableConstraintsOnApplyChanges, DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            if (schema.Tables == null || !schema.HasTables)
                throw new MissingTablesException();

            // get Database builder
            var builder = this.GetDatabaseBuilder();
            builder.UseChangeTracking = this.UseChangeTracking;
            builder.UseBulkProcedures = this.SupportBulkOperations;

            // Disable check constraints
            if (disableConstraintsOnApplyChanges)
                foreach (var table in schema.Tables.Reverse())
                    await this.DisableConstraintsAsync(context, table, connection, transaction).ConfigureAwait(false);

            // Sorting tables based on dependencies between them
            var schemaTables = schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            foreach (var schemaTable in schemaTables)
            {
                var tableBuilder = this.GetTableBuilder(schemaTable);
                // set if the builder supports creating the bulk operations proc stock
                tableBuilder.UseBulkProcedures = this.SupportBulkOperations;
                tableBuilder.UseChangeTracking = this.UseChangeTracking;

                // adding filter
                this.AddFilters(schemaTable, tableBuilder);

                await tableBuilder.DropAsync(provision, connection, transaction).ConfigureAwait(false);

                // Interceptor
                await this.Orchestrator.InterceptAsync(new TableDeprovisionedArgs(context, provision, schemaTable, connection, transaction), cancellationToken).ConfigureAwait(false);
            }

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
        public virtual async Task<SyncContext> ProvisionAsync(SyncContext context, SyncSet schema, SyncProvision provision, string scopeInfoTableName,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            if (schema.Tables == null || !schema.HasTables)
                throw new MissingTablesException();

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
                var tableBuilder = this.GetTableBuilder(schemaTable);
                // set if the builder supports creating the bulk operations proc stock
                tableBuilder.UseBulkProcedures = this.SupportBulkOperations;
                tableBuilder.UseChangeTracking = this.UseChangeTracking;

                // adding filter
                this.AddFilters(schemaTable, tableBuilder);

                // Interceptor
                await this.Orchestrator.InterceptAsync(new TableProvisioningArgs(context, provision, tableBuilder, connection, transaction), cancellationToken).ConfigureAwait(false);

                await tableBuilder.CreateAsync(provision, connection, transaction).ConfigureAwait(false);
                await tableBuilder.CreateForeignKeysAsync(connection, transaction).ConfigureAwait(false);

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
            }

        }

    }
}
