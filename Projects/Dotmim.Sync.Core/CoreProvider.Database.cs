using Dotmim.Sync.Builders;

using Dotmim.Sync.Enumerations;

using Dotmim.Sync.Messages;
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

            // Event progress
            context.SyncStage = SyncStage.SchemaDeprovisioning;

            var script = new StringBuilder();

            var beforeArgs = new DatabaseDeprovisioningArgs(context, provision, schema, connection, transaction);
            await this.InterceptAsync(beforeArgs).ConfigureAwait(false);

            // get Database builder
            var builder = this.GetDatabaseBuilder();
            builder.UseChangeTracking = this.UseChangeTracking;
            builder.UseBulkProcedures = this.SupportBulkOperations;

            // Disable check constraints
            if (disableConstraintsOnApplyChanges)
                foreach (var table in schema.Tables.Reverse())
                    this.DisableConstraints(context, table, connection, transaction);

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

                context.SyncStage = SyncStage.TableSchemaDeprovisioning;

                // Launch any interceptor if available
                await this.InterceptAsync(new TableDeprovisioningArgs(context, provision, schemaTable, connection, transaction)).ConfigureAwait(false);

                tableBuilder.Drop(provision, connection, transaction);

                // Report & Interceptor
                context.SyncStage = SyncStage.TableSchemaDeprovisioned;
                var tableDeprovisionedArgs = new TableDeprovisionedArgs(context, provision, schemaTable, connection, transaction);
                this.ReportProgress(context, progress, tableDeprovisionedArgs);
                await this.InterceptAsync(tableDeprovisionedArgs).ConfigureAwait(false);
            }

            if (provision.HasFlag(SyncProvision.ClientScope))
                context = await this.DropClientScopeAsync(context, scopeInfoTableName, connection, transaction, cancellationToken, progress);

            if (provision.HasFlag(SyncProvision.ServerScope))
                context = await this.DropServerScopeAsync(context, scopeInfoTableName, connection, transaction, cancellationToken, progress);

            if (provision.HasFlag(SyncProvision.ServerHistoryScope))
                context = await this.DropServerHistoryScopeAsync(context, scopeInfoTableName, connection, transaction, cancellationToken, progress);


            // Report & Interceptor
            context.SyncStage = SyncStage.SchemaDeprovisioned;
            var args = new DatabaseDeprovisionedArgs(context, provision, schema, script.ToString(), connection, transaction);
            this.ReportProgress(context, progress, args);
            await this.InterceptAsync(args).ConfigureAwait(false);

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

            // Event progress
            context.SyncStage = SyncStage.SchemaProvisioning;

            var script = new StringBuilder();

            var beforeArgs = new DatabaseProvisioningArgs(context, provision, schema, connection, transaction);
            await this.InterceptAsync(beforeArgs).ConfigureAwait(false);

            // get Database builder
            var builder = this.GetDatabaseBuilder();
            builder.UseChangeTracking = this.UseChangeTracking;
            builder.UseBulkProcedures = this.SupportBulkOperations;

            // Initialize database if needed
            builder.EnsureDatabase(connection, transaction);

            // Shoudl we create scope
            if (provision.HasFlag(SyncProvision.ClientScope))
                context = await this.EnsureClientScopeAsync(context, scopeInfoTableName, connection, transaction, cancellationToken, progress);

            if (provision.HasFlag(SyncProvision.ServerScope))
                context = await this.EnsureServerScopeAsync(context, scopeInfoTableName, connection, transaction, cancellationToken, progress);

            if (provision.HasFlag(SyncProvision.ServerHistoryScope))
                context = await this.EnsureServerHistoryScopeAsync(context, scopeInfoTableName, connection, transaction, cancellationToken, progress);


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

                context.SyncStage = SyncStage.TableSchemaProvisioning;

                // Launch any interceptor if available
                await this.InterceptAsync(new TableProvisioningArgs(context, provision, schemaTable, connection, transaction)).ConfigureAwait(false);

                tableBuilder.Create(provision, connection, transaction);
                tableBuilder.CreateForeignKeys(connection, transaction);

                // Report & Interceptor
                context.SyncStage = SyncStage.TableSchemaProvisioned;
                var tableProvisionedArgs = new TableProvisionedArgs(context, provision, schemaTable, connection, transaction);
                this.ReportProgress(context, progress, tableProvisionedArgs);
                await this.InterceptAsync(tableProvisionedArgs).ConfigureAwait(false);
            }

            // Report & Interceptor
            context.SyncStage = SyncStage.SchemaProvisioned;
            var args = new DatabaseProvisionedArgs(context, provision, schema, script.ToString(), connection, transaction);
            this.ReportProgress(context, progress, args);
            await this.InterceptAsync(args).ConfigureAwait(false);

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
