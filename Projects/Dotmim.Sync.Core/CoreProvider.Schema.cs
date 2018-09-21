using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using Dotmim.Sync.Messages;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class CoreProvider
    {

        /// <summary>
        /// Generate the DmTable configuration from a given columns list
        /// Validate that all columns are currently supported by the provider
        /// </summary>
        private void ValidateTableFromColumns(DmTable dmTable, IEnumerable<DmColumn> columns, IDbManagerTable dbManagerTable)
        {
            dmTable.OriginalProvider = this.ProviderTypeName;

            var ordinal = 0;

            // Eventually, do not raise exception here, just we don't have any columns
            if (columns == null || columns.Any() == false)
                return;

            // Get PrimaryKey
            var dmTableKeys = dbManagerTable.GetTablePrimaryKeys();

            if (dmTableKeys == null || dmTableKeys.Any() == false)
                throw new MissingPrimaryKeyException($"No Primary Keys in table {dmTable.TableName}, Can't make a synchronization with a table without primary keys.");

            //// Check if we have more than one column (excepting primarykeys)
            //var columnsNotPkeys = columns.Count(c => !dmTableKeys.Contains(c.ColumnName));

            //if (columnsNotPkeys <= 0)
            //    throw new NotSupportedException($"{dmTable.TableName} does not contains any columns, excepting primary keys.");

            // Delete all existing columns
            if (dmTable.PrimaryKey != null && dmTable.PrimaryKey.Columns != null && dmTable.PrimaryKey.Columns.Length > 0)
                dmTable.PrimaryKey = new DmKey();

            if (dmTable.Columns.Count > 0)
                dmTable.Columns.Clear();

            foreach (var column in columns.OrderBy(c => c.Ordinal))
            {
                // First of all validate if the column is currently supported
                if (!Metadata.IsValid(column))
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

                dmTable.Columns.Add(column);

                // Validate max length
                column.MaxLength = Metadata.ValidateMaxLength(column.OriginalTypeName, column.IsUnsigned, column.IsUnicode, column.MaxLength);

                // Gets the datastore owner dbType (could be SqlDbtype, MySqlDbType, SqliteDbType, NpgsqlDbType & so on ...)
                object datastoreDbType = Metadata.ValidateOwnerDbType(column.OriginalTypeName, column.IsUnsigned, column.IsUnicode, column.MaxLength);

                // once we have the datastore type, we can have the managed type
                Type columnType = Metadata.ValidateType(datastoreDbType);

                // and the DbType
                column.DbType = Metadata.ValidateDbType(column.OriginalTypeName, column.IsUnsigned, column.IsUnicode, column.MaxLength);

                // Gets the owner dbtype (SqlDbType, OracleDbType, MySqlDbType, NpsqlDbType & so on ...)
                // Sqlite does not have it's own type, so it's DbType too
                column.OriginalDbType = datastoreDbType.ToString();

                // Validate if column should be readonly
                column.IsReadOnly = Metadata.ValidateIsReadonly(column);

                // set position ordinal
                column.SetOrdinal(ordinal);
                ordinal++;

                // Validate the precision and scale properties
                if (Metadata.IsNumericType(column.OriginalTypeName))
                {
                    if (Metadata.SupportScale(column.OriginalTypeName))
                    {
                        var (p, s) = Metadata.ValidatePrecisionAndScale(column);
                        column.Precision = p;
                        column.PrecisionSpecified = true;
                        column.Scale = s;
                        column.ScaleSpecified = true;
                    }
                    else
                    {
                        column.Precision = Metadata.ValidatePrecision(column);
                        column.PrecisionSpecified = true;
                        column.ScaleSpecified = false;
                    }

                }

            }

            DmColumn[] columnsForKey = new DmColumn[dmTableKeys.Count()];

            int i = 0;
            foreach (var rowColumn in dmTableKeys)
            {
                var columnKey = dmTable.Columns.FirstOrDefault(c => String.Equals(c.ColumnName, rowColumn, StringComparison.InvariantCultureIgnoreCase));
                columnsForKey[i++] = columnKey ?? throw new MissingPrimaryKeyException("Primary key found is not present in the columns list");
            }

            // Set the primary Key
            dmTable.PrimaryKey = new DmKey(columnsForKey);
        }

        /// <summary>
        /// Create a simple configuration, based on tables
        /// </summary>
        private async Task<SyncConfiguration> ReadSchemaAsync(string[] tables)
        {
            // Load the configuration
            var configuration = new SyncConfiguration(tables);
            await this.ReadSchemaAsync(configuration.Schema);
            return configuration;
        }

        /// <summary>
        /// update configuration object with tables desc from server database
        /// </summary>
        private async Task ReadSchemaAsync(DmSet schema)
        {
            if (schema == null || schema.Tables.Count <= 0)
                throw new ArgumentNullException("syncConfiguration", "Configuration should contains Tables, at least tables with a name");

            DbConnection connection = null;
            DbTransaction transaction;

            try
            {
                var relations = new List<DbRelationDefinition>(20);
                var syncConfiguration = schema.Tables;
                using (connection = this.CreateConnection())
                {
                    await connection.OpenAsync();

                    using (transaction = connection.BeginTransaction())
                    {
                        foreach (var dmTable in syncConfiguration)
                        {
                            var builderTable = this.GetDbManager(dmTable.TableName);
                            var tblManager = builderTable.CreateManagerTable(connection, transaction);

                            // get columns list
                            var lstColumns = tblManager.GetTableDefinition();

                            // Validate the column list and get the dmTable configuration object.
                            this.ValidateTableFromColumns(dmTable, lstColumns, tblManager);

                            relations.AddRange(tblManager.GetTableRelations());
                        }

                        transaction.Commit();
                    }

                    connection.Close();
                }
                if (relations.Any())
                {
                    foreach (var r in relations)
                    {
                        var dmTable = schema.Tables[r.TableName];
                        var tblColumns = r.KeyColumnsName
                            .Select(kc => dmTable.Columns[kc])
                            .ToArray();

                        var foreignTable = syncConfiguration[r.ReferenceTableName];

                        // Since we can have a table with a foreign key but not the parent table
                        // It's not a problem, just forget it
                        if (foreignTable == null || foreignTable.Columns.Count == 0)
                            continue;

                        var foreignColumns = r.ReferenceColumnsName
                             .Select(fc => foreignTable.Columns[fc])
                             .ToArray();

                        if (foreignColumns == null || foreignColumns.Any(c => c == null))
                            throw new NotSupportedException(
                                $"Foreign columns {string.Join(",", r.ReferenceColumnsName)} does not exist in table {r.ReferenceTableName}");

                        var dmRelation = new DmRelation(r.ForeignKey, tblColumns, foreignColumns);

                        schema.Relations.Add(dmRelation);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SyncException(ex, SyncStage.SchemaApplying, this.ProviderTypeName);
            }
            finally
            {
                if (connection != null && connection.State != ConnectionState.Closed)
                    connection.Close();
            }
        }

        /// <summary>
        /// Ensure configuration is correct on both server and client side
        /// </summary>
        public virtual async Task<(SyncContext, DmSet)> EnsureSchemaAsync(SyncContext context, MessageEnsureSchema message)
        {
            try
            {
                context.SyncStage = SyncStage.SchemaApplying;

                // Get cache manager and try to get configuration from cache
                //var cacheManager = this.CacheManager;

                //// if we don't pass config object (configuration == null), we may be in proxy mode, so the config object is handled by a local configuration object.
                //if (syncConfiguration == null && this.syncConfiguration == null)
                //    throw new ArgumentNullException("syncConfiguration", "You try to set a provider with no configuration object");

                //// the configuration has been set from the proxy server itself, use it.
                //if (syncConfiguration == null && this.syncConfiguration != null)
                //    syncConfiguration = this.syncConfiguration;

                // Raise event before
                context.SyncStage = SyncStage.SchemaApplying;
                var beforeArgs2 = new SchemaApplyingEventArgs(this.ProviderTypeName, context.SyncStage, message.Schema);
                this.TryRaiseProgressEvent(beforeArgs2, this.SchemaApplying);
                bool overWriteConfiguration = beforeArgs2.OverwriteConfiguration;


                // if we dont have already read the tables || we want to overwrite the current config
                if ((message.Schema.HasTables && !message.Schema.HasColumns))
                    await this.ReadSchemaAsync(message.Schema);

                context.SyncStage = SyncStage.SchemaApplied;
                var afterArgs = new SchemaAppliedEventArgs(this.ProviderTypeName, context.SyncStage, message.Schema);
                this.TryRaiseProgressEvent(afterArgs, this.SchemaApplied);

                return (context, message.Schema);
            }
            catch (SyncException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new SyncException(ex, SyncStage.SchemaApplying, this.ProviderTypeName);
            }

        }

        ///// <summary>
        ///// Get cached configuration (inmemory or session cache)
        ///// </summary>
        //public SyncConfiguration GetCacheConfiguration()
        //{
        //    var configurationSurrogate = this.CacheManager.GetValue<DmSetSurrogate>(SYNC_CONF);
        //    if (configurationSurrogate == null)
        //        return null;

        //    var dmSet = configurationSurrogate.ConvertToDmSet();
        //    if (dmSet == null)
        //        return null;

        //    return SyncConfiguration.DeserializeFromDmSet(dmSet);
        //}

        //public void SetCacheConfiguration(SyncConfiguration configuration)
        //{
        //    var dmSetConf = new DmSet();
        //    SyncConfiguration.SerializeInDmSet(dmSetConf, configuration);
        //    var dmSSetConf = new DmSetSurrogate(dmSetConf);
        //    this.CacheManager.Set(SYNC_CONF, dmSSetConf);
        //}
    }
}
