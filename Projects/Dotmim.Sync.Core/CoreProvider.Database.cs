using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Filter;
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
        public async Task DeprovisionAsync(SyncSet schema, SyncProvision provision, string scopeInfoTableName = SyncOptions.DefaultScopeInfoTableName)
        {
            DbConnection connection = null;
            DbTransaction transaction = null;
            try
            {
                if (schema.Tables == null || !schema.HasTables)
                    throw new ArgumentNullException("tables", "You must set the tables you want to provision");


                // Open the connection
                using (connection = this.CreateConnection())
                {
                    await connection.OpenAsync().ConfigureAwait(false);

                    // Let provider knows a connection is opened
                    this.OnConnectionOpened(connection);

                    await this.InterceptAsync(new ConnectionOpenArgs(null, connection)).ConfigureAwait(false);

                    using (transaction = connection.BeginTransaction())
                    {
                        await this.InterceptAsync(new TransactionOpenArgs(null, connection, transaction)).ConfigureAwait(false);

                        // Launch any interceptor if available
                        await this.InterceptAsync(new DatabaseDeprovisioningArgs(null, provision, schema, connection, transaction)).ConfigureAwait(false);

                        for (var i = schema.Tables.Count - 1; i >= 0; i--)
                        {
                            // Get the table
                            var schemaTable = schema.Tables[i];

                            // call any interceptor
                            await this.InterceptAsync(new TableDeprovisioningArgs(null, provision, schemaTable, connection, transaction)).ConfigureAwait(false);

                            // get the builder
                            var builder = this.GetDatabaseBuilder(schemaTable);
                            builder.UseBulkProcedures = this.SupportBulkOperations;

                            // adding filters
                            this.AddFilters(schemaTable, builder);

                            if (provision.HasFlag(SyncProvision.TrackingTable) || provision.HasFlag(SyncProvision.All))
                                builder.DropTrackingTable(connection, transaction);

                            if (provision.HasFlag(SyncProvision.StoredProcedures) || provision.HasFlag(SyncProvision.All))
                                builder.DropProcedures(connection, transaction);

                            if (provision.HasFlag(SyncProvision.Triggers) || provision.HasFlag(SyncProvision.All))
                                builder.DropTriggers(connection, transaction);

                            // On purpose, the flag SyncProvision.All does not include the SyncProvision.Table, too dangerous...
                            if (provision.HasFlag(SyncProvision.Table))
                                builder.DropTable(connection, transaction);

                            // call any interceptor
                            await this.InterceptAsync(new TableDeprovisionedArgs(null, provision, schemaTable, connection, transaction)).ConfigureAwait(false);
                        }

                        if (provision.HasFlag(SyncProvision.Scope) || provision.HasFlag(SyncProvision.All))
                        {
                            var scopeBuilder = this.GetScopeBuilder().CreateScopeInfoBuilder(scopeInfoTableName, connection, transaction);
                            if (!scopeBuilder.NeedToCreateScopeInfoTable())
                                scopeBuilder.DropScopeInfoTable();
                        }

                        // Launch any interceptor if available
                        await this.InterceptAsync(new DatabaseDeprovisionedArgs(null, provision, schema, null, connection, transaction)).ConfigureAwait(false);

                        await this.InterceptAsync(new TransactionCommitArgs(null, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SyncException(ex, SyncStage.SchemaApplying);
            }
            finally
            {
                if (connection != null && connection.State != ConnectionState.Closed)
                    connection.Close();

                await this.InterceptAsync(new ConnectionCloseArgs(null, connection, transaction)).ConfigureAwait(false);

                // Let provider knows a connection is closed
                this.OnConnectionClosed(connection);
            }

        }

        /// <summary>
        /// Deprovision a database
        /// </summary>
        public async Task ProvisionAsync(SyncSet schema, SyncProvision provision, string scopeInfoTableName = SyncOptions.DefaultScopeInfoTableName)
        {
            DbConnection connection = null;
            DbTransaction transaction = null;

            try
            {
                if (schema.Tables == null || !schema.HasTables)
                    throw new ArgumentNullException("tables", "You must set the tables you want to provision");


                // Open the connection
                using (connection = this.CreateConnection())
                {
                    await connection.OpenAsync().ConfigureAwait(false);

                    // Let provider knows a connection is opened
                    this.OnConnectionOpened(connection);

                    await this.InterceptAsync(new ConnectionOpenArgs(null, connection)).ConfigureAwait(false);

                    using (transaction = connection.BeginTransaction())
                    {
                        await this.InterceptAsync(new TransactionOpenArgs(null, connection, transaction)).ConfigureAwait(false);

                        var beforeArgs =
                            new DatabaseProvisioningArgs(null, provision, schema, connection, transaction);

                        // Launch any interceptor if available
                        await this.InterceptAsync(beforeArgs).ConfigureAwait(false);

                        if (provision.HasFlag(SyncProvision.Scope) || provision.HasFlag(SyncProvision.All))
                        {
                            var scopeBuilder = this.GetScopeBuilder().CreateScopeInfoBuilder(scopeInfoTableName, connection, transaction);
                            if (scopeBuilder.NeedToCreateScopeInfoTable())
                                scopeBuilder.CreateScopeInfoTable();
                        }

                        // Sorting tables based on dependencies between them
                        var schemaTables = schema.Tables
                            .SortByDependencies(tab => tab.GetRelations()
                                .Select(r => r.GetParentTable()));

                        foreach (var schemaTable in schemaTables)
                        {
                            // get the builder
                            var builder = this.GetDatabaseBuilder(schemaTable);

                            // call any interceptor
                            await this.InterceptAsync(new TableProvisioningArgs(null, provision, schemaTable, connection, transaction)).ConfigureAwait(false);

                            builder.UseBulkProcedures = this.SupportBulkOperations;

                            // adding filters
                            this.AddFilters(schemaTable, builder);

                            // On purpose, the flag SyncProvision.All does not include the SyncProvision.Table, too dangerous...
                            if (provision.HasFlag(SyncProvision.Table))
                                builder.CreateTable(connection, transaction);

                            if (provision.HasFlag(SyncProvision.TrackingTable) || provision.HasFlag(SyncProvision.All))
                                builder.CreateTrackingTable(connection, transaction);

                            if (provision.HasFlag(SyncProvision.Triggers) || provision.HasFlag(SyncProvision.All))
                                builder.CreateTriggers(connection, transaction);

                            if (provision.HasFlag(SyncProvision.StoredProcedures) || provision.HasFlag(SyncProvision.All))
                                builder.CreateStoredProcedures(connection, transaction);

                            // call any interceptor
                            await this.InterceptAsync(new TableProvisionedArgs(null, provision, schemaTable, connection, transaction)).ConfigureAwait(false);

                        }

                        // call any interceptor
                        await this.InterceptAsync(new DatabaseProvisionedArgs(null, provision, schema, null, connection, transaction)).ConfigureAwait(false);
                        await this.InterceptAsync(new TransactionCommitArgs(null, connection, transaction)).ConfigureAwait(false);

                        transaction.Commit();
                    }

                    connection.Close();
                }

            }
            catch (Exception ex)
            {
                throw new SyncException(ex, SyncStage.SchemaApplying);
            }
            finally
            {
                if (connection != null && connection.State != ConnectionState.Closed)
                    connection.Close();

                await this.InterceptAsync(new ConnectionCloseArgs(null, connection, transaction)).ConfigureAwait(false);

                // Let provider knows a connection is closed
                this.OnConnectionClosed(connection);
            }
        }

        /// <summary>
        /// Be sure all tables are ready and configured for sync
        /// the ScopeSet Configuration MUST be filled by the schema form Database
        /// </summary>
        public virtual async Task<SyncContext> EnsureDatabaseAsync(SyncContext context, SyncSet schema,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            try
            {

                // Event progress
                context.SyncStage = SyncStage.SchemaApplying;

                var script = new StringBuilder();

                var beforeArgs = new DatabaseProvisioningArgs(context, SyncProvision.All, schema, connection, transaction);
                await this.InterceptAsync(beforeArgs).ConfigureAwait(false);

                // Sorting tables based on dependencies between them
                var schemaTables = schema.Tables
                    .SortByDependencies(tab => tab.GetRelations()
                        .Select(r => r.GetParentTable()));

                foreach (var schemaTable in schemaTables)
                {
                    var builder = this.GetDatabaseBuilder(schemaTable);
                    // set if the builder supports creating the bulk operations proc stock
                    builder.UseBulkProcedures = this.SupportBulkOperations;

                    // adding filter
                    this.AddFilters(schemaTable, builder);

                    context.SyncStage = SyncStage.TableSchemaApplying;

                    // Launch any interceptor if available
                    await this.InterceptAsync(new TableProvisioningArgs(context, SyncProvision.All, schemaTable, connection, transaction)).ConfigureAwait(false);

                    string currentScript = null;
                    if (beforeArgs.GenerateScript)
                    {
                        currentScript = builder.ScriptTable(connection, transaction);
                        currentScript += builder.ScriptForeignKeys(connection, transaction);
                        script.Append(currentScript);
                    }

                    builder.Create(connection, transaction);
                    builder.CreateForeignKeys(connection, transaction);

                    // Report & Interceptor
                    context.SyncStage = SyncStage.TableSchemaApplied;
                    var tableProvisionedArgs = new TableProvisionedArgs(context, SyncProvision.All, schemaTable, connection, transaction);
                    this.ReportProgress(context, progress, tableProvisionedArgs);
                    await this.InterceptAsync(tableProvisionedArgs).ConfigureAwait(false);
                }

                // Report & Interceptor
                context.SyncStage = SyncStage.SchemaApplied;
                var args = new DatabaseProvisionedArgs(context, SyncProvision.All, schema, script.ToString(), connection, transaction);
                this.ReportProgress(context, progress, args);
                await this.InterceptAsync(args).ConfigureAwait(false);

                await this.InterceptAsync(new TransactionCommitArgs(context, connection, transaction)).ConfigureAwait(false);

                return context;

            }
            catch (Exception ex)
            {
                throw new SyncException(ex, SyncStage.SchemaApplying);
            }
        }

        /// <summary>
        /// Adding filters to an existing configuration
        /// </summary>
        private void AddFilters(SyncTable schemaTable, DbBuilder builder)
        {
            var schema = schemaTable.Schema;

            if (schema.Filters != null && schema.Filters.Count > 0)
            {
                // get the all the filters for the table
                var tableFilters = schemaTable.GetFilters();

                if (tableFilters == null)
                    return;

                foreach (var filter in tableFilters)
                {
                    // Get the column
                    var columnFilter = schemaTable.Columns.FirstOrDefault(c => c.ColumnName.Equals(filter.ColumnName, SyncGlobalization.DataSourceStringComparison));

                    if (columnFilter == null && !filter.IsVirtual)
                        throw new InvalidExpressionException($"Column {filter.ColumnName} does not exist in Table {schemaTable.TableName}");

                    builder.FilterColumns.Add(new SyncFilter(schemaTable.TableName, columnFilter.ColumnName, schemaTable.SchemaName, filter.ColumnType));
                }
            }

        }

    }
}
