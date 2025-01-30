using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Contains the logic to get the schema from the database.
    /// </summary>
    public abstract partial class BaseOrchestrator
    {

        /// <summary>
        /// Read the schema stored from the orchestrator database, through the provider.
        /// <example>
        /// Example:
        /// <code>
        ///  var remoteOrchestrator = new RemoteOrchestrator(serverProvider);
        ///  var setup = new SyncSetup("ProductCategory", "Product");
        ///  var schema = await remoteOrchestrator.GetSchemaAsync(scopeName, setup);
        /// </code>
        /// </example>
        /// </summary>
        /// <returns>Schema containing tables, columns, relations, primary keys.</returns>
        public virtual async Task<SyncSet> GetSchemaAsync(string scopeName, SyncSetup setup, DbConnection connection = null, DbTransaction transaction = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {
                if (setup == null || setup.Tables.Count <= 0)
                    return null;

                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    SyncSet schema;
                    (context, schema) = await this.InternalGetSchemaAsync(context, setup,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    return schema;
                }
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }

        /// <inheritdoc cref="GetSchemaAsync(string, SyncSetup, DbConnection, DbTransaction)"/>
        public virtual Task<SyncSet> GetSchemaAsync(SyncSetup setup, DbConnection connection = null, DbTransaction transaction = null)
            => this.GetSchemaAsync(SyncOptions.DefaultScopeName, setup, connection, transaction);

        /// <summary>
        /// Read all tables from database. Don't need a setup to get tables. This method returns all tables whatever they are tracked or not.
        /// <example>
        /// Example:
        /// <code>
        ///  var remoteOrchestrator = new RemoteOrchestrator(serverProvider);
        ///  var setup = await remoteOrchestrator.GetAllTablesAsync()
        /// </code>
        /// </example>
        /// </summary>
        /// <returns>SyncSetup containing tables names and column names.</returns>
        public virtual async Task<SyncSetup> GetAllTablesAsync(DbConnection connection = null, DbTransaction transaction = null)
        {
            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);
            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None, connection, transaction).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    SyncSetup setup;
                    (context, setup) = await this.InternalGetAllTablesAsync(
                        context,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    return setup;
                }
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// update configuration object with tables desc from server database.
        /// </summary>
        internal async Task<(SyncContext Context, SyncSet Schema)> InternalGetSchemaAsync(SyncContext context, SyncSetup setup, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                if (setup == null || setup.Tables.Count <= 0)
                    throw new MissingTablesException();

                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.Provisioning, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    await this.InterceptAsync(new SchemaLoadingArgs(context, setup, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    // Create the schema
                    var schema = new SyncSet();

                    // copy filters from setup
                    foreach (var filter in setup.Filters)
                        schema.Filters.Add(filter);

                    var relations = new List<DbRelationDefinition>(20);

                    foreach (var setupTable in setup.Tables)
                    {
                        var (syncTable, tableRelations) = await this.InternalGetTableSchemaAsync(context, setupTable, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                        // Add this table to schema
                        schema.Tables.Add(syncTable);

                        // Since we are not sure of the order of reading tables
                        // create a tmp relations list
                        relations.AddRange(tableRelations);
                    }

                    // Parse and affect relations to schema
                    this.SetRelations(context, relations, schema);

                    // Ensure all objects have correct relations to schema
                    schema.EnsureSchema();

                    var schemaArgs = new SchemaLoadedArgs(context, schema, runner.Connection);
                    await this.InterceptAsync(schemaArgs, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    return (context, schema);
                }
            }
            catch (Exception exception)
            {
                throw this.GetSyncError(context, exception);
            }
        }

        /// <summary>
        /// Get all tables with column names from database.
        /// </summary>
        internal async Task<(SyncContext Context, SyncSetup Setup)> InternalGetAllTablesAsync(SyncContext context, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.Provisioning, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    var dbBuilder = this.Provider.GetDatabaseBuilder();

                    var setup = await dbBuilder.GetAllTablesAsync(runner.Connection, runner.Transaction).ConfigureAwait(false);

                    return (context, setup);
                }
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }

        private async Task<(SyncTable SyncTable, IEnumerable<DbRelationDefinition> Relations)> InternalGetTableSchemaAsync(
            SyncContext context, SetupTable setupTable, DbConnection connection, DbTransaction transaction,
            IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {

                if (this.Provider == null)
                    throw new MissingProviderException(nameof(this.InternalGetTableSchemaAsync));

                // ensure table is compliante with name / schema with provider
                var syncTable = await this.Provider.GetDatabaseBuilder().EnsureTableAsync(setupTable.TableName, setupTable.SchemaName, connection, transaction).ConfigureAwait(false);

                // tmp scope info
                var scopeInfo = InternalCreateScopeInfo(context.ScopeName);
                scopeInfo.Setup = new SyncSetup();
                scopeInfo.Setup.Tables.Add(setupTable);

                var syncAdapter = this.Provider.GetSyncAdapter(syncTable, scopeInfo);
                var tableBuilder = syncAdapter.GetTableBuilder();

                bool exists;
                (context, exists) = await this.InternalExistsTableAsync(scopeInfo, context, tableBuilder, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                if (!exists)
                    throw new MissingTableException(setupTable.TableName, setupTable.SchemaName, connection.Database);

                // get columns list
                var lstColumns = await tableBuilder.GetColumnsAsync(connection, transaction).ConfigureAwait(false);

                // Validate the column list and get the dmTable configuration object.
                this.FillSyncTableWithColumns(context, setupTable, syncTable, lstColumns);

                // Check primary Keys
                await this.SetPrimaryKeysAsync(context, syncTable, tableBuilder, connection, transaction).ConfigureAwait(false);

                // get all relations
                var tableRelations = await tableBuilder.GetRelationsAsync(connection, transaction).ConfigureAwait(false);

                return (syncTable, tableRelations);
            }
            catch (Exception ex)
            {
                string message = null;

                if (setupTable != null)
                    message += $"Table:{setupTable.GetFullName()}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Generate the DmTable configuration from a given columns list
        /// Validate that all columns are currently supported by the provider.
        /// </summary>
        private void FillSyncTableWithColumns(SyncContext context, SetupTable setupTable, SyncTable schemaTable, IEnumerable<SyncColumn> columns)
        {
            try
            {
                schemaTable.OriginalProvider = this.Provider.GetProviderTypeName();

                var ordinal = 0;

                // Eventually, do not raise exception here, just we don't have any columns
                if (columns == null)
                    return;

                var columnsList = columns.ToList();

                if (columnsList.Count == 0)
                    return;

                // Delete all existing columns
                if (schemaTable.PrimaryKeys.Count > 0)
                    schemaTable.PrimaryKeys.Clear();

                if (schemaTable.Columns.Count > 0)
                    schemaTable.Columns.Clear();

                List<SyncColumn> lstColumns;

                // Validate columns list from setup table if any
                if (setupTable.Columns != null && setupTable.Columns.Count > 1)
                {
                    lstColumns = new List<SyncColumn>();

                    foreach (var setupColumn in setupTable.Columns)
                    {
                        // Check if the columns list contains the column name we specified in the setup
                        var column = columnsList.FirstOrDefault(c => c.ColumnName.Equals(setupColumn, SyncGlobalization.DataSourceStringComparison));

                        if (column == null)
                            throw new MissingColumnException(setupColumn, schemaTable.TableName);
                        else
                            ((List<SyncColumn>)lstColumns).Add(column);
                    }
                }
                else
                {
                    lstColumns = columnsList;
                }

                foreach (var column in lstColumns.OrderBy(c => c.Ordinal))
                {
                    // First of all validate if the column is currently supported
                    if (!this.Provider.GetMetadata().IsValid(column))
                        throw new UnsupportedColumnTypeException(setupTable.GetFullName(), column.ColumnName, column.OriginalTypeName, this.Provider.GetProviderTypeName());

                    var columnNameLower = column.ColumnName.ToLowerInvariant();
                    if (columnNameLower == "sync_scope_id"
                        || columnNameLower == "changeTable"
                        || columnNameLower == "sync_scope_name"
                        || columnNameLower == "sync_min_timestamp"
                        || columnNameLower == "sync_row_count"
                        || columnNameLower == "sync_force_write"
                        || columnNameLower == "sync_update_scope_id"
                        || columnNameLower == "sync_timestamp"
                        || columnNameLower == "sync_row_is_tombstone")
                        throw new UnsupportedColumnNameException(setupTable.GetFullName(), column.ColumnName, column.OriginalTypeName, this.Provider.GetProviderTypeName());

                    // Gets the max length
                    column.MaxLength = this.Provider.GetMetadata().GetMaxLength(column);

                    // Gets the owner dbtype (SqlDbType, OracleDbType, MySqlDbType, NpsqlDbType & so on ...)
                    // Sqlite does not have it's own type, so it's DbType too
                    column.OriginalDbType = this.Provider.GetMetadata().GetOwnerDbType(column).ToString();

                    // get the downgraded DbType
                    column.DbType = (int)this.Provider.GetMetadata().GetDbType(column);

                    // Gets the column readonly's propertye
                    column.IsReadOnly = this.Provider.GetMetadata().IsReadonly(column);

                    // set position ordinal
                    column.Ordinal = ordinal;
                    ordinal++;

                    // Validate the precision and scale properties
                    if (this.Provider.GetMetadata().IsNumericType(column))
                    {
                        if (this.Provider.GetMetadata().IsSupportingScale(column))
                        {
                            var (p, s) = this.Provider.GetMetadata().GetPrecisionAndScale(column);
                            column.Precision = p;
                            column.PrecisionIsSpecified = true;
                            column.Scale = s;
                            column.ScaleIsSpecified = true;
                        }
                        else
                        {
                            column.Precision = this.Provider.GetMetadata().GetPrecision(column);
                            column.PrecisionIsSpecified = true;
                            column.ScaleIsSpecified = false;
                        }
                    }

                    // Get the managed type
                    // Important to set it at the end, because we are altering column.DataType here
                    column.SetType(this.Provider.GetMetadata().GetType(column));

                    // if setup table has no columns, we add all columns from db
                    // otherwise check if columns exist in the data source
                    if (setupTable.Columns == null || setupTable.Columns.Count <= 0 || setupTable.Columns.Contains(column.ColumnName))
                        schemaTable.Columns.Add(column);

                    // If column does not allow null value and is not compute
                    // We will not be able to insert a row, so raise an error
                    else if (!column.AllowDBNull && !column.IsCompute && !column.IsReadOnly && string.IsNullOrEmpty(column.DefaultValue))
                        throw new Exception($"In table {setupTable.GetFullName()}, column {column.ColumnName} is not part of your setup. But it seems this columns is mandatory in your data source.");
                }
            }
            catch (Exception ex)
            {
                string message = null;

                if (setupTable != null)
                    message += $"Table:{setupTable.GetFullName()}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Check then add primary keys to schema table.
        /// </summary>
        private async Task SetPrimaryKeysAsync(SyncContext context, SyncTable schemaTable, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction)
        {
            try
            {

                // Get PrimaryKey
                var schemaPrimaryKeys = await tableBuilder.GetPrimaryKeysAsync(connection, transaction).ConfigureAwait(false);

                if (schemaPrimaryKeys == null || schemaPrimaryKeys.Any() == false)
                    throw new MissingPrimaryKeyException(schemaTable.TableName);

                // Set the primary Key
                foreach (var rowColumn in schemaPrimaryKeys.OrderBy(r => r.Ordinal))
                {
                    // Find the column in the schema columns
                    var columnKey = schemaTable.Columns.FirstOrDefault(sc => sc.EqualsByName(rowColumn));

                    if (columnKey == null)
                        throw new MissingPrimaryKeyColumnException(rowColumn.ColumnName, schemaTable.TableName);

                    var columnNameLower = columnKey.ColumnName.ToLowerInvariant();
                    if (columnNameLower == "update_scope_id"
                        || columnNameLower == "timestamp"
                        || columnNameLower == "timestamp_bigint"
                        || columnNameLower == "sync_row_is_tombstone"
                        || columnNameLower == "last_change_datetime")
                        throw new UnsupportedPrimaryKeyColumnNameException(schemaTable.GetFullName(), columnKey.ColumnName, columnKey.OriginalTypeName, this.Provider.GetProviderTypeName());

                    schemaTable.PrimaryKeys.Add(columnKey.ColumnName);
                }

                // Get all auto increment columns from the table and check if they are primary keys
                foreach (var column in schemaTable.Columns.Where(c => c.IsAutoIncrement))
                {
                    if (schemaTable.PrimaryKeys.Contains(column.ColumnName))
                        continue;

                    throw new InvalidColumnAutoIncrementException(column.ColumnName, schemaTable.TableName);
                }
            }
            catch (Exception ex)
            {
                string message = null;

                if (tableBuilder != null && tableBuilder.TableDescription != null)
                    message += $"Table:{tableBuilder.TableDescription.GetFullName()}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// For all relations founded, create the SyncRelation and add it to schema.
        /// </summary>
        private void SetRelations(SyncContext context, List<DbRelationDefinition> relations, SyncSet schema)
        {
            try
            {

                if (relations == null || relations.Count <= 0)
                    return;

                foreach (var r in relations)
                {
                    // Get table from the relation where we need to work on
                    var schemaTable = schema.Tables[r.TableName, r.SchemaName];

                    // get SchemaColumn from SchemaTable, based on the columns from relations
                    var schemaColumns = r.Columns.OrderBy(kc => kc.Order)
                        .Select(kc =>
                        {
                            var schemaColumn = schemaTable.Columns[kc.KeyColumnName];

                            if (schemaColumn == null)
                                return null;

                            return new SyncColumnIdentifier(schemaColumn.ColumnName, schemaTable.TableName, schemaTable.SchemaName);
                        })
                        .Where(sc => sc != null)
                        .ToList();

                    // if we don't find the column, maybe we just dont have this column in our setup def
                    if (schemaColumns == null || schemaColumns.Count == 0)
                        continue;

                    // then Get the foreign table as well
                    var foreignTable = schemaTable.Schema.Tables[r.ReferenceTableName, r.ReferenceSchemaName];

                    // Since we can have a table with a foreign key but not the parent table
                    // It's not a problem, just forget it
                    if (foreignTable == null || foreignTable.Columns.Count == 0)
                        continue;

                    var foreignColumns = r.Columns.OrderBy(kc => kc.Order)
                         .Select(fc =>
                         {
                             var schemaColumn = foreignTable.Columns[fc.ReferenceColumnName];
                             if (schemaColumn == null)
                                 return null;
                             return new SyncColumnIdentifier(schemaColumn.ColumnName, foreignTable.TableName, foreignTable.SchemaName);
                         })
                         .Where(sc => sc != null)
                         .ToList();

                    if (foreignColumns == null || foreignColumns.Count == 0)
                        continue;

                    var schemaRelation = new SyncRelation(r.ForeignKey, schemaColumns, foreignColumns);

                    schema.Relations.Add(schemaRelation);
                }
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }
    }
}