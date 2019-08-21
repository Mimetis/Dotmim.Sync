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
        public async Task DeprovisionAsync(SyncConfiguration configuration, SyncProvision provision)
        {
            DbConnection connection = null;
            DbTransaction transaction = null;
            try
            {
                if (configuration.Schema == null || !configuration.Schema.HasTables)
                    throw new ArgumentNullException("tables", "You must set the tables you want to provision");


                // Open the connection
                using (connection = this.CreateConnection())
                {
                    await connection.OpenAsync();
                    await this.InterceptAsync(new ConnectionOpenArgs(null, connection));

                    using (transaction = connection.BeginTransaction())
                    {
                        await this.InterceptAsync(new TransactionOpenArgs(null, connection, transaction));

                        // Load the configuration
                        this.ReadSchema(configuration.Schema, connection, transaction);

                        // Launch any interceptor if available
                        await this.InterceptAsync(new DatabaseDeprovisioningArgs(null, provision, configuration.Schema, connection, transaction));

                        for (var i = configuration.Count - 1; i >= 0; i--)
                        {
                            // Get the table
                            var dmTable = configuration.Schema.Tables[i];

                            // call any interceptor
                            await this.InterceptAsync(new TableDeprovisioningArgs(null, provision, dmTable, connection, transaction));

                            // get the builder
                            var builder = this.GetDatabaseBuilder(dmTable);
                            builder.UseBulkProcedures = this.SupportBulkOperations;

                            // adding filters
                            this.AddFilters(configuration.Filters, dmTable, builder);

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
                            await this.InterceptAsync(new TableDeprovisionedArgs(null, provision, dmTable, connection, transaction));
                        }

                        if (provision.HasFlag(SyncProvision.Scope) || provision.HasFlag(SyncProvision.All))
                        {
                            var scopeBuilder = this.GetScopeBuilder().CreateScopeInfoBuilder(configuration.ScopeInfoTableName, connection, transaction);
                            if (!scopeBuilder.NeedToCreateScopeInfoTable())
                                scopeBuilder.DropScopeInfoTable();
                        }

                        // Launch any interceptor if available
                        await this.InterceptAsync(new DatabaseDeprovisionedArgs(null, provision, configuration.Schema, null, connection, transaction));

                        await this.InterceptAsync(new TransactionCommitArgs(null, connection, transaction));
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

                await this.InterceptAsync(new ConnectionCloseArgs(null, connection, transaction));
            }

        }

        /// <summary>
        /// Deprovision a database
        /// </summary>
        public async Task ProvisionAsync(SyncConfiguration configuration, SyncProvision provision)
        {
            DbConnection connection = null;
            DbTransaction transaction = null;

            try
            {
                if (configuration.Schema == null || !configuration.Schema.HasTables)
                    throw new ArgumentNullException("tables", "You must set the tables you want to provision");


                // Open the connection
                using (connection = this.CreateConnection())
                {
                    await connection.OpenAsync();
                    await this.InterceptAsync(new ConnectionOpenArgs(null, connection));

                    using (transaction = connection.BeginTransaction())
                    {
                        await this.InterceptAsync(new TransactionOpenArgs(null, connection, transaction));

                        // Load the configuration
                        this.ReadSchema(configuration.Schema, connection, transaction);

                        var beforeArgs =
                            new DatabaseProvisioningArgs(null, provision, configuration.Schema, connection, transaction);

                        // Launch any interceptor if available
                        await this.InterceptAsync(beforeArgs);

                        if (provision.HasFlag(SyncProvision.Scope) || provision.HasFlag(SyncProvision.All))
                        {
                            var scopeBuilder = this.GetScopeBuilder().CreateScopeInfoBuilder(configuration.ScopeInfoTableName, connection, transaction);
                            if (scopeBuilder.NeedToCreateScopeInfoTable())
                                scopeBuilder.CreateScopeInfoTable();
                        }

                        // Sorting tables based on dependencies between them
                        var dmTables = configuration.Schema.Tables
                            .SortByDependencies(tab => tab.ChildRelations
                                .Select(r => r.ChildTable));

                        foreach (var dmTable in dmTables)
                        {
                            // get the builder
                            var builder = this.GetDatabaseBuilder(dmTable);

                            // call any interceptor
                            await this.InterceptAsync(new TableProvisioningArgs(null, provision, dmTable, connection, transaction));

                            builder.UseBulkProcedures = this.SupportBulkOperations;

                            // adding filters
                            this.AddFilters(configuration.Filters, dmTable, builder);

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
                            await this.InterceptAsync(new TableProvisionedArgs(null, provision, dmTable, connection, transaction));

                        }

                        // call any interceptor
                        await this.InterceptAsync(new DatabaseProvisionedArgs(null, provision, configuration.Schema, null, connection, transaction));
                        await this.InterceptAsync(new TransactionCommitArgs(null, connection, transaction));

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

                await this.InterceptAsync(new ConnectionCloseArgs(null, connection, transaction));
            }
        }

        /// <summary>
        /// Be sure all tables are ready and configured for sync
        /// the ScopeSet Configuration MUST be filled by the schema form Database
        /// </summary>
        public virtual async Task<SyncContext> EnsureDatabaseAsync(SyncContext context, MessageEnsureDatabase message)
        {
            DbConnection connection = null;
            DbTransaction transaction = null;
            try
            {
                // Event progress
                context.SyncStage = SyncStage.SchemaApplying;

                var script = new StringBuilder();

                // Open the connection
                using (connection = this.CreateConnection())
                {
                    await connection.OpenAsync();
                    await this.InterceptAsync(new ConnectionOpenArgs(context, connection));

                    using (transaction = connection.BeginTransaction())
                    {
                        // Interceptors
                        await this.InterceptAsync(new TransactionOpenArgs(context, connection, transaction));

                        var beforeArgs = new DatabaseProvisioningArgs(context, SyncProvision.All, message.Schema, connection, transaction);
                        await this.InterceptAsync(beforeArgs);

                        if (message.ScopeInfo.LastSync.HasValue && !beforeArgs.OverwriteSchema)
                            return context;

                        // Sorting tables based on dependencies between them
                        var dmTables = message.Schema.Tables
                            .SortByDependencies(tab => tab.ChildRelations
                                .Select(r => r.ChildTable));

                        foreach (var dmTable in dmTables)
                        {
                            var builder = this.GetDatabaseBuilder(dmTable);
                            // set if the builder supports creating the bulk operations proc stock
                            builder.UseBulkProcedures = this.SupportBulkOperations;

                            // adding filter
                            this.AddFilters(message.Filters, dmTable, builder);

                            context.SyncStage = SyncStage.TableSchemaApplying;

                            // Launch any interceptor if available
                            await this.InterceptAsync(new TableProvisioningArgs(context, SyncProvision.All, dmTable, connection, transaction));

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
                            var tableProvisionedArgs = new TableProvisionedArgs(context, SyncProvision.All, dmTable, connection, transaction);
                            this.ReportProgress(context, tableProvisionedArgs);
                            await this.InterceptAsync(tableProvisionedArgs);
                        }

                        // Report & Interceptor
                        context.SyncStage = SyncStage.SchemaApplied;
                        var args = new DatabaseProvisionedArgs(context, SyncProvision.All, message.Schema, script.ToString(), connection, transaction);
                        this.ReportProgress(context, args);
                        await this.InterceptAsync(args);

                        await this.InterceptAsync(new TransactionCommitArgs(context, connection, transaction));
                        transaction.Commit();
                    }

                    connection.Close();

                    return context;
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

                await this.InterceptAsync(new ConnectionCloseArgs(context, connection, transaction));
            }
        }

        /// <summary>
        /// Adding filters to an existing configuration
        /// </summary>
        private void AddFilters(ICollection<FilterClause> filters, DmTable dmTable, DbBuilder builder)
        {
            if (filters != null && filters.Count > 0)
            {
                var tableFilters = filters.Where(f => dmTable.TableName.Equals(f.TableName, StringComparison.InvariantCultureIgnoreCase));

                foreach (var filter in tableFilters)
                {
                    var columnFilter = dmTable.Columns[filter.ColumnName];

                    if (columnFilter == null && !filter.IsVirtual)
                        throw new InvalidExpressionException($"Column {filter.ColumnName} does not exist in Table {dmTable.TableName}");

                    builder.FilterColumns.Add(new FilterClause(filter.TableName, filter.ColumnName, filter.ColumnType));
                }
            }

        }

    }
}
