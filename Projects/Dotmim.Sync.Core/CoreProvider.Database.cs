using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Data;
using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using System.Text;
using Dotmim.Sync.Filter;
using System.Collections.Generic;
using Dotmim.Sync.Messages;

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
            try
            {
                if (configuration.Schema == null || !configuration.Schema.HasTables)
                    throw new ArgumentNullException("tables", "You must set the tables you want to provision");

                // Load the configuration
                await this.ReadSchemaAsync(configuration.Schema);

                // Open the connection
                using (connection = this.CreateConnection())
                {
                    await connection.OpenAsync();

                    using (var transaction = connection.BeginTransaction())
                    {
                        for (int i = configuration.Count - 1; i >= 0; i--)
                        {
                            // Get the table
                            var dmTable = configuration.Schema.Tables[i];

                            // get the builder
                            var builder = GetDatabaseBuilder(dmTable);
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
                        }

                        if (provision.HasFlag(SyncProvision.Scope) || provision.HasFlag(SyncProvision.All))
                        {
                            var scopeBuilder = GetScopeBuilder().CreateScopeInfoBuilder(configuration.ScopeInfoTableName, connection, transaction);
                            if (!scopeBuilder.NeedToCreateScopeInfoTable())
                                scopeBuilder.DropScopeInfoTable();
                        }
                        transaction.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SyncException(ex, SyncStage.DatabaseApplying, this.ProviderTypeName);
            }
            finally
            {
                if (connection != null && connection.State != ConnectionState.Closed)
                    connection.Close();
            }

        }

        /// <summary>
        /// Deprovision a database
        /// </summary>
        public async Task ProvisionAsync(SyncConfiguration configuration, SyncProvision provision)
        {
            DbConnection connection = null;

            try
            {
                if (configuration.Schema == null || !configuration.Schema.HasTables)
                    throw new ArgumentNullException("tables", "You must set the tables you want to provision");

                // Load the configuration
                await this.ReadSchemaAsync(configuration.Schema);

                // Open the connection
                using (connection = this.CreateConnection())
                {
                    await connection.OpenAsync();

                    using (var transaction = connection.BeginTransaction())
                    {

                        if (provision.HasFlag(SyncProvision.Scope) || provision.HasFlag(SyncProvision.All))
                        {
                            var scopeBuilder = GetScopeBuilder().CreateScopeInfoBuilder(configuration.ScopeInfoTableName, connection, transaction);
                            if (scopeBuilder.NeedToCreateScopeInfoTable())
                                scopeBuilder.CreateScopeInfoTable();
                        }

                        for (int i = 0; i < configuration.Count; i++)
                        {
                            // Get the table
                            var dmTable = configuration.Schema.Tables[i];

                            // get the builder
                            var builder = GetDatabaseBuilder(dmTable);
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

                        }
                        transaction.Commit();
                    }

                    connection.Close();
                }

            }
            catch (Exception ex)
            {
                throw new SyncException(ex, SyncStage.DatabaseApplying, this.ProviderTypeName);
            }
            finally
            {
                if (connection != null && connection.State != ConnectionState.Closed)
                    connection.Close();
            }
        }

        /// <summary>
        /// Be sure all tables are ready and configured for sync
        /// the ScopeSet Configuration MUST be filled by the schema form Database
        /// </summary>
        public virtual async Task<SyncContext> EnsureDatabaseAsync(SyncContext context, MessageEnsureDatabase message)
        {
            DbConnection connection = null;
            try
            {
                // Event progress
                context.SyncStage = SyncStage.DatabaseApplying;

                DatabaseApplyingEventArgs beforeArgs =
                    new DatabaseApplyingEventArgs(this.ProviderTypeName, context.SyncStage, message.Schema);
                this.TryRaiseProgressEvent(beforeArgs, this.DatabaseApplying);

                // If scope exists and lastdatetime sync is present, so database exists
                // Check if we don't have an OverwriteConfiguration (if true, we force the check)

                if (message.ScopeInfo.LastSync.HasValue && !beforeArgs.OverwriteSchema)
                    return context;

                StringBuilder script = new StringBuilder();

                // Open the connection
                using (connection = this.CreateConnection())
                {
                    await connection.OpenAsync();

                    using (var transaction = connection.BeginTransaction())
                    {
                        // Sorting tables based on dependencies between them
                        var dmTables = message.Schema.Tables
                            .SortByDependencies(tab => tab.ChildRelations
                                .Select(r => r.ChildTable));

                        foreach (var dmTable in dmTables)
                        {
                            var builder = GetDatabaseBuilder(dmTable);
                            // set if the builder supports creating the bulk operations proc stock
                            builder.UseBulkProcedures = this.SupportBulkOperations;
                           
                            // adding filter
                            this.AddFilters(message.Filters, dmTable, builder);

                            context.SyncStage = SyncStage.DatabaseTableApplying;
                            DatabaseTableApplyingEventArgs beforeTableArgs =
                                new DatabaseTableApplyingEventArgs(this.ProviderTypeName, context.SyncStage, dmTable.TableName);
                            this.TryRaiseProgressEvent(beforeTableArgs, this.DatabaseTableApplying);

                            string currentScript = null;
                            if (beforeArgs.GenerateScript)
                            {
                                currentScript = builder.ScriptTable(connection, transaction);
                                currentScript += builder.ScriptForeignKeys(connection, transaction);
                                script.Append(currentScript);
                            }

                            builder.Create(connection, transaction);
                            builder.CreateForeignKeys(connection, transaction);

                            context.SyncStage = SyncStage.DatabaseTableApplied;
                            DatabaseTableAppliedEventArgs afterTableArgs =
                                new DatabaseTableAppliedEventArgs(this.ProviderTypeName, context.SyncStage, dmTable.TableName, currentScript);
                            this.TryRaiseProgressEvent(afterTableArgs, this.DatabaseTableApplied);
                        }

                        context.SyncStage = SyncStage.DatabaseApplied;
                        var afterArgs = new DatabaseAppliedEventArgs(this.ProviderTypeName, context.SyncStage, script.ToString());
                        this.TryRaiseProgressEvent(afterArgs, this.DatabaseApplied);

                        transaction.Commit();
                    }

                    connection.Close();

                    return context;
                }

            }
            catch (Exception ex)
            {
                throw new SyncException(ex, SyncStage.DatabaseApplying, this.ProviderTypeName);
            }
            finally
            {
                if (connection != null && connection.State != ConnectionState.Closed)
                    connection.Close();
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
