
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class BaseOrchestrator
    {

        /// <summary>
        /// Read the schema stored from the orchestrator database, through the provider.
        /// </summary>
        /// <returns>Schema containing tables, columns, relations, primary keys</returns>
        public virtual Task<SyncSet> GetSchemaAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => RunInTransactionAsync(SyncStage.SchemaReading, async (ctx, connection, transaction) =>
            {
                var schema = await this.InternalGetSchemaAsync(ctx, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                return schema;

            });

        /// <summary>
        /// update configuration object with tables desc from server database
        /// </summary>
        internal async Task<SyncSet> InternalGetSchemaAsync(SyncContext context, DbConnection connection, DbTransaction transaction,
                                                           CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            if (this.Setup == null || this.Setup.Tables.Count <= 0)
                throw new MissingTablesException();

            this.logger.LogInformation(SyncEventsId.GetSchema, this.Setup);

            await this.InterceptAsync(new SchemaLoadingArgs(context, this.Setup, connection, transaction), cancellationToken).ConfigureAwait(false);

            // Create the schema
            var schema = new SyncSet();

            // copy filters from setup
            foreach (var filter in this.Setup.Filters)
                schema.Filters.Add(filter);

            var relations = new List<DbRelationDefinition>(20);

            foreach (var setupTable in this.Setup.Tables)
            {
                this.logger.LogDebug(SyncEventsId.GetSchema, setupTable);

                // creating an empty schema table with just table name / schema name.
                // will be enough to make the exist table verification
                var syncTable = new SyncTable(setupTable.TableName, setupTable.SchemaName);
                var tableBuilder = this.Provider.GetTableBuilder(syncTable, this.Setup);

                var exists = await InternalExistsTableAsync(context, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    throw new MissingTableException(string.IsNullOrEmpty(setupTable.SchemaName) ? setupTable.TableName : setupTable.SchemaName + "." + setupTable.TableName);

                // get columns list
                var lstColumns = await tableBuilder.GetColumnsAsync(connection, transaction).ConfigureAwait(false);

                if (this.logger.IsEnabled(LogLevel.Debug))
                    foreach (var col in lstColumns)
                        this.logger.LogDebug(SyncEventsId.GetSchema, col);

                // Validate the column list and get the dmTable configuration object.
                this.FillSyncTableWithColumns(setupTable, syncTable, lstColumns);

                // Add this table to schema
                schema.Tables.Add(syncTable);

                // Check primary Keys
                await SetPrimaryKeysAsync(syncTable, tableBuilder, connection, transaction).ConfigureAwait(false);

                // get all relations
                var tableRelations = await tableBuilder.GetRelationsAsync(connection, transaction).ConfigureAwait(false);

                // Since we are not sure of the order of reading tables
                // create a tmp relations list
                relations.AddRange(tableRelations);
            }

            // Parse and affect relations to schema
            SetRelations(relations, schema);

            // Ensure all objects have correct relations to schema
            schema.EnsureSchema();

            var schemaArgs = new SchemaLoadedArgs(context, schema, connection);
            await this.InterceptAsync(schemaArgs, cancellationToken).ConfigureAwait(false);
            this.ReportProgress(context, progress, schemaArgs);

            return schema;
        }

        /// <summary>
        /// Generate the DmTable configuration from a given columns list
        /// Validate that all columns are currently supported by the provider
        /// </summary>
        private void FillSyncTableWithColumns(SetupTable setupTable, SyncTable schemaTable, IEnumerable<SyncColumn> columns)
        {
            schemaTable.OriginalProvider = this.Provider.ProviderTypeName;
            schemaTable.SyncDirection = setupTable.SyncDirection;

            var ordinal = 0;

            // Eventually, do not raise exception here, just we don't have any columns
            if (columns == null || columns.Any() == false)
                return;

            // Delete all existing columns
            if (schemaTable.PrimaryKeys.Count > 0)
                schemaTable.PrimaryKeys.Clear();

            if (schemaTable.Columns.Count > 0)
                schemaTable.Columns.Clear();


            IEnumerable<SyncColumn> lstColumns;

            // Validate columns list from setup table if any
            if (setupTable.Columns != null && setupTable.Columns.Count > 1)
            {
                lstColumns = new List<SyncColumn>();

                foreach (var setupColumn in setupTable.Columns)
                {
                    // Check if the columns list contains the column name we specified in the setup
                    var column = columns.FirstOrDefault(c => c.ColumnName.Equals(setupColumn, SyncGlobalization.DataSourceStringComparison));

                    if (column == null)
                        throw new MissingColumnException(setupColumn, schemaTable.TableName);
                    else
                        ((List<SyncColumn>)lstColumns).Add(column);
                }
            }
            else
            {
                lstColumns = columns;
            }


            foreach (var column in lstColumns.OrderBy(c => c.Ordinal))
            {
                // First of all validate if the column is currently supported
                if (!this.Provider.Metadata.IsValid(column))
                    throw new UnsupportedColumnTypeException(column.ColumnName, column.OriginalTypeName, this.Provider.ProviderTypeName);

                var columnNameLower = column.ColumnName.ToLowerInvariant();
                if (columnNameLower == "sync_scope_name"
                    || columnNameLower == "scope_timestamp"
                    || columnNameLower == "scope_is_local"
                    || columnNameLower == "scope_last_sync"
                    || columnNameLower == "create_scope_id"
                    || columnNameLower == "update_scope_id"
                    || columnNameLower == "create_timestamp"
                    || columnNameLower == "update_timestamp"
                    || columnNameLower == "timestamp"
                    || columnNameLower == "sync_row_is_tombstone"
                    || columnNameLower == "last_change_datetime"
                    || columnNameLower == "sync_scope_name"
                    || columnNameLower == "sync_scope_name"
                    )
                    throw new UnsupportedColumnTypeException(column.ColumnName, column.OriginalTypeName, this.Provider.ProviderTypeName);

                // Validate max length
                column.MaxLength = this.Provider.Metadata.ValidateMaxLength(column.OriginalTypeName, column.IsUnsigned, column.IsUnicode, column.MaxLength);

                // Gets the datastore owner dbType (could be SqlDbtype, MySqlDbType, SqliteDbType, NpgsqlDbType & so on ...)
                var datastoreDbType = this.Provider.Metadata.ValidateOwnerDbType(column.OriginalTypeName, column.IsUnsigned, column.IsUnicode, column.MaxLength);

                // once we have the datastore type, we can have the managed type
                var columnType = this.Provider.Metadata.ValidateType(datastoreDbType);

                // and the DbType
                column.DbType = (int)this.Provider.Metadata.ValidateDbType(column.OriginalTypeName, column.IsUnsigned, column.IsUnicode, column.MaxLength);

                // Gets the owner dbtype (SqlDbType, OracleDbType, MySqlDbType, NpsqlDbType & so on ...)
                // Sqlite does not have it's own type, so it's DbType too
                column.OriginalDbType = datastoreDbType.ToString();

                // Validate if column should be readonly
                column.IsReadOnly = this.Provider.Metadata.ValidateIsReadonly(column);

                // set position ordinal
                column.Ordinal = ordinal;
                ordinal++;

                // Validate the precision and scale properties
                if (this.Provider.Metadata.IsNumericType(column.OriginalTypeName))
                {
                    if (this.Provider.Metadata.SupportScale(column.OriginalTypeName))
                    {
                        var (p, s) = this.Provider.Metadata.ValidatePrecisionAndScale(column);
                        column.Precision = p;
                        column.PrecisionSpecified = true;
                        column.Scale = s;
                        column.ScaleSpecified = true;
                    }
                    else
                    {
                        column.Precision = this.Provider.Metadata.ValidatePrecision(column);
                        column.PrecisionSpecified = true;
                        column.ScaleSpecified = false;
                    }

                }

                // if setup table has no columns, we add all columns from db
                // otherwise check if columns exist in the data source
                if (setupTable.Columns == null || setupTable.Columns.Count <= 0 || setupTable.Columns.Contains(column.ColumnName))
                    schemaTable.Columns.Add(column);
                // If column does not allow null value and is not compute
                // We will not be able to insert a row, so raise an error
                else if (!column.AllowDBNull && !column.IsCompute && !column.IsReadOnly && string.IsNullOrEmpty(column.DefaultValue))
                    throw new Exception($"Column {column.ColumnName} is not part of your setup. But it seems this columns is mandatory in your data source.");

            }
        }

        /// <summary>
        /// Check then add primary keys to schema table
        /// </summary>
        private async Task SetPrimaryKeysAsync(SyncTable schemaTable, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction)
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

                schemaTable.PrimaryKeys.Add(columnKey.ColumnName);
            }
        }

        /// <summary>
        /// For all relations founded, create the SyncRelation and add it to schema
        /// </summary>
        private void SetRelations(List<DbRelationDefinition> relations, SyncSet schema)
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


    }
}
