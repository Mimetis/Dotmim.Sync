using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using Dotmim.Sync.Messages;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class CoreProvider
    {

        /// <summary>
        /// update configuration object with tables desc from server database
        /// </summary>
        public SyncSet ReadSchema(SyncSetup setup, DbConnection connection, DbTransaction transaction)
        {
            if (setup == null || setup.Tables.Count <= 0)
                throw new ArgumentNullException(nameof(setup), "You should add, at least on table to the sync process.");

            // Create the schema
            var schema = new SyncSet(setup.ScopeName)
            {
                StoredProceduresPrefix = setup.StoredProceduresPrefix,
                StoredProceduresSuffix = setup.StoredProceduresSuffix,
                TrackingTablesPrefix = setup.TrackingTablesPrefix,
                TrackingTablesSuffix = setup.TrackingTablesSuffix,
                TriggersPrefix = setup.TriggersPrefix,
                TriggersSuffix = setup.TriggersSuffix,
            };

            // copy filters from setup
            foreach (var filter in setup.Filters)
                schema.Filters.Add(filter.TableName, filter.ColumnName, filter.SchemaName, filter.ColumnType);

            var relations = new List<DbRelationDefinition>(20);

            foreach (var setupTable in setup.Tables)
            {
                var builderTable = this.GetDbManager(setupTable.TableName, setupTable.SchemaName);
                var tblManager = builderTable.CreateManagerTable(connection, transaction);

                // Check if table exists
                var syncTable = tblManager.GetTable();

                if (syncTable == null)
                    throw new SyncException($"Table {(string.IsNullOrEmpty(setupTable.SchemaName) ? "" : setupTable.SchemaName + ".")}{setupTable.TableName} does not exists.", SyncStage.SchemaReading, SyncExceptionType.Data);

                // get columns list
                var lstColumns = tblManager.GetColumns();

                // Validate the column list and get the dmTable configuration object.
                this.FillSyncTableWithColumns(setupTable, syncTable, lstColumns, tblManager);

                // Add this table to schema
                schema.Tables.Add(syncTable);

                // Check primary Keys
                SetPrimaryKeys(syncTable, tblManager);

                // get all relations
                var tableRelations = tblManager.GetRelations();

                // Since we are not sure of the order of reading tables
                // create a tmp relations list
                relations.AddRange(tableRelations);
            }

            // Parse and affect relations to schema
            SetRelations(relations, schema);

            return schema;
        }


        /// <summary>
        /// Ensure configuration is correct on both server and client side
        /// </summary>
        public virtual async Task<(SyncContext, SyncSet)> EnsureSchemaAsync(SyncContext context, SyncSetup setup,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                context.SyncStage = SyncStage.SchemaReading;

                var schema = this.ReadSchema(setup, connection, transaction);

                // Progress & Interceptor
                context.SyncStage = SyncStage.SchemaRead;
                var schemaArgs = new SchemaArgs(context, schema, connection, transaction);
                this.ReportProgress(context, progress, schemaArgs);
                await this.InterceptAsync(schemaArgs).ConfigureAwait(false);

                return (context, schema);
            }
            catch (SyncException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new SyncException(ex, SyncStage.SchemaReading);
            }
        }



        /// <summary>
        /// Generate the DmTable configuration from a given columns list
        /// Validate that all columns are currently supported by the provider
        /// </summary>
        private void FillSyncTableWithColumns(SetupTable setupTable, SyncTable schemaTable, IEnumerable<SyncColumn> columns, IDbManagerTable dbManagerTable)
        {
            schemaTable.OriginalProvider = this.ProviderTypeName;

            var ordinal = 0;

            // Eventually, do not raise exception here, just we don't have any columns
            if (columns == null || columns.Any() == false)
                return;

            // Delete all existing columns
            if (schemaTable.PrimaryKeys.Count > 0)
                schemaTable.PrimaryKeys.Clear();

            if (schemaTable.Columns.Count > 0)
                schemaTable.Columns.Clear();

            foreach (var column in columns.OrderBy(c => c.Ordinal))
            {
                // First of all validate if the column is currently supported
                if (!this.Metadata.IsValid(column))
                    throw new NotSupportedException($"The Column {column.ColumnName} of type {column.OriginalTypeName} from provider {this.ProviderTypeName} is not currently supported.");

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
                    throw new NotSupportedException($"The Column name {column.ColumnName} from provider {this.ProviderTypeName} is a reserved column name. Please choose another column name.");

                // Validate max length
                column.MaxLength = this.Metadata.ValidateMaxLength(column.OriginalTypeName, column.IsUnsigned, column.IsUnicode, column.MaxLength);

                // Gets the datastore owner dbType (could be SqlDbtype, MySqlDbType, SqliteDbType, NpgsqlDbType & so on ...)
                var datastoreDbType = this.Metadata.ValidateOwnerDbType(column.OriginalTypeName, column.IsUnsigned, column.IsUnicode, column.MaxLength);

                // once we have the datastore type, we can have the managed type
                var columnType = this.Metadata.ValidateType(datastoreDbType);


                // and the DbType
                column.DbType = (int)this.Metadata.ValidateDbType(column.OriginalTypeName, column.IsUnsigned, column.IsUnicode, column.MaxLength);

                // Gets the owner dbtype (SqlDbType, OracleDbType, MySqlDbType, NpsqlDbType & so on ...)
                // Sqlite does not have it's own type, so it's DbType too
                column.OriginalDbType = datastoreDbType.ToString();

                // Validate if column should be readonly
                column.IsReadOnly = this.Metadata.ValidateIsReadonly(column);

                // set position ordinal
                column.Ordinal = ordinal;
                ordinal++;

                // Validate the precision and scale properties
                if (this.Metadata.IsNumericType(column.OriginalTypeName))
                {
                    if (this.Metadata.SupportScale(column.OriginalTypeName))
                    {
                        var (p, s) = this.Metadata.ValidatePrecisionAndScale(column);
                        column.Precision = p;
                        column.PrecisionSpecified = true;
                        column.Scale = s;
                        column.ScaleSpecified = true;
                    }
                    else
                    {
                        column.Precision = this.Metadata.ValidatePrecision(column);
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
                else if (!column.AllowDBNull && !column.IsCompute && !column.IsReadOnly)
                    throw new Exception($"Column {column.ColumnName} is not part of your setup. But it seems this columns is mandatory in your data source.");

            }
        }

        /// <summary>
        /// Check then add primary keys to schema table
        /// </summary>
        private void SetPrimaryKeys(SyncTable schemaTable, IDbManagerTable dbManagerTable)
        {
            // Get PrimaryKey
            var schemaPrimaryKeys = dbManagerTable.GetPrimaryKeys();

            if (schemaPrimaryKeys == null || schemaPrimaryKeys.Any() == false)
                throw new MissingPrimaryKeyException($"No Primary Keys in table {schemaTable.TableName}, Can't make a synchronization with a table without primary keys.");

            // Set the primary Key
            foreach (var rowColumn in schemaPrimaryKeys.OrderBy(r => r.Ordinal))
            {
                var columnKey = schemaTable.Columns.FirstOrDefault(sc => sc == rowColumn);

                if (columnKey == null)
                    throw new ArgumentException($"Column {rowColumn.ColumnName} does not exist in {schemaTable.TableName}");

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
                    });

                // if we don't find the column, maybe we just dont have this column in our setup def
                if (schemaColumns == null || schemaColumns.Count() == 0)
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
                     });


                if (foreignColumns == null || foreignColumns.Any(c => c == null))
                    continue;

                var schemaRelation = new SyncRelation(r.ForeignKey, schemaColumns.ToList(), foreignColumns.ToList());

                schema.Relations.Add(schemaRelation);
            }
        }


    }
}
