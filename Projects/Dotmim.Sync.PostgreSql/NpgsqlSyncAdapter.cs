using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.PostgreSql.Builders;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using static Npgsql.Replication.PgOutput.Messages.RelationMessage;

namespace Dotmim.Sync.PostgreSql.Builders
{
    public class NpgsqlSyncAdapter : DbSyncAdapter
    {
        public static DateTime SqlDateMin = new DateTime(1753, 1, 1);
        public static DateTime SqlSmallDateMin = new DateTime(1900, 1, 1);
        private static ConcurrentDictionary<string, List<NpgsqlParameter>> derivingParameters
             = new ConcurrentDictionary<string, List<NpgsqlParameter>>();

        private string scopeName;
        private SyncSetup setup;
        private SyncTable tableDescription;
        private ParserName tableName;
        private ParserName trackingTableName;
        public NpgsqlSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName, bool useBulkOperations) : base(tableDescription, setup, scopeName, useBulkOperations)
        {
            this.tableDescription = tableDescription;
            this.tableName = tableName;
            this.trackingTableName = trackingTableName;
            this.setup = setup;
            this.scopeName = scopeName;
            this.UseBulkOperations = useBulkOperations;
            this.NpgsqlObjectNames = new NpgsqlObjectNames(tableDescription, tableName, trackingTableName, setup, scopeName);
            this.SqlMetadata = new NpgsqlDbMetadata();
        }

        public NpgsqlObjectNames NpgsqlObjectNames { get; set; }
        public NpgsqlDbMetadata SqlMetadata { get; set; }
        public override async Task AddCommandParametersAsync(DbCommandType commandType, DbCommand command, DbConnection connection, DbTransaction transaction, SyncFilter filter = null)
        {
            if (command == null)
                return;

            if (command.Parameters != null && command.Parameters.Count > 0)
                return;

            // special case for constraint
            if (commandType == DbCommandType.DisableConstraints || commandType == DbCommandType.EnableConstraints)
                return;


            // special case for UpdateMetadata
            if (commandType == DbCommandType.UpdateMetadata)
            {
                this.SetUpdateRowParameters(command);
                return;
            }
            if (commandType == DbCommandType.SelectMetadata)
            {
                this.SetSelectRowParameters(command);
                return;
            }
            if (commandType == DbCommandType.SelectChanges || commandType == DbCommandType.SelectChangesWithFilters ||
                commandType == DbCommandType.SelectInitializedChanges || commandType == DbCommandType.SelectInitializedChangesWithFilters)
            {
                this.SetSelectChangesParameters(command, commandType, filter);
                return;
            }

            // if we don't have stored procedure, return, because we don't want to derive parameters
            if (command.CommandType != CommandType.StoredProcedure)
                return;

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                command.Transaction = transaction;

                var textParser = ParserName.Parse(command.CommandText).Unquoted().Normalized().ToString();

                var source = connection.Database;

                textParser = $"{source}-{textParser}";

                if (derivingParameters.ContainsKey(textParser))
                {
                    foreach (var p in derivingParameters[textParser])
                        command.Parameters.Add(p.Clone());
                }
                else
                {
                    // Using the SqlCommandBuilder.DeriveParameters() method is not working yet, 
                    // because default value is not well done handled on the Dotmim.Sync framework
                    // TODO: Fix SqlCommandBuilder.DeriveParameters
                    //SqlCommandBuilder.DeriveParameters((SqlCommand)command);

                    //await ((NpgsqlConnection)connection).DeriveParametersAsync((NpgsqlCommand)command, false, (NpgsqlTransaction)transaction).ConfigureAwait(false);
                    NpgsqlCommandBuilder.DeriveParameters((NpgsqlCommand)command);
                    var arrayParameters = new List<NpgsqlParameter>();
                    foreach (var p in command.Parameters)
                        arrayParameters.Add(((NpgsqlParameter)p).Clone());

                    derivingParameters.TryAdd(textParser, arrayParameters);
                }

                if (command.Parameters.Count > 0 && command.Parameters[0].ParameterName == "@RETURN_VALUE")
                    command.Parameters.RemoveAt(0);

            }
            catch (Exception ex)
            {
                Debug.WriteLine($"DeriveParameters failed : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();
            }


            foreach (var parameter in command.Parameters)
            {
                var sqlParameter = (NpgsqlParameter)parameter;

                // try to get the source column (from the SchemaTable)
                var sqlParameterName = sqlParameter.ParameterName.Replace("@", "");
                var colDesc = TableDescription.Columns.FirstOrDefault(c => c.ColumnName.Equals(sqlParameterName, SyncGlobalization.DataSourceStringComparison));

                if (colDesc != null && !string.IsNullOrEmpty(colDesc.ColumnName))
                    sqlParameter.SourceColumn = colDesc.ColumnName;
            }
        }

        public override Task ExecuteBatchCommandAsync(DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> arrayItems, SyncTable schemaChangesTable, SyncTable failedRows, long? lastTimestamp, DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
        //public override async Task ExecuteBatchCommandAsync(DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> applyRows, 
        //                                                    SyncTable schemaChangesTable, SyncTable failedRows, long? lastTimestamp, 
        //                                                    DbConnection connection, DbTransaction transaction)
        //{
        //    var applyRowsCount = applyRows.Count();

        //    if (applyRowsCount <= 0)
        //        return;

        //    var syncRowState = SyncRowState.None;

        //    var records = new List<DbDataRecord>(applyRowsCount);
        //    NpgsqlDbMetadata[] metadatas = new NpgsqlDbMetadata[schemaChangesTable.Columns.Count];

        //    for (int i = 0; i < schemaChangesTable.Columns.Count; i++)
        //        metadatas[i] = GetSqlMetadaFromType(schemaChangesTable.Columns[i]);

        //    try
        //    {
        //        foreach (var row in applyRows)
        //        {
        //            syncRowState = row.RowState;

        //            NpgsqlDbMetadata record = new NpgsqlDbMetadata(metadatas);

        //            int sqlMetadataIndex = 0;

        //            for (int i = 0; i < schemaChangesTable.Columns.Count; i++)
        //            {
        //                var schemaColumn = schemaChangesTable.Columns[i];

        //                // Get the default value
        //                //var columnType = schemaColumn.GetDataType();
        //                dynamic defaultValue = schemaColumn.GetDefaultValue();
        //                dynamic rowValue = row[i];

        //                // metadatas don't have readonly values, so get from sqlMetadataIndex
        //                var sqlMetadataType = metadatas[sqlMetadataIndex].SqlDbType;

        //                if (rowValue != null)
        //                {
        //                    var columnType = rowValue.GetType();

        //                    switch (sqlMetadataType)
        //                    {
        //                        case SqlDbType.BigInt:
        //                            if (columnType != typeof(long))
        //                                rowValue = SyncTypeConverter.TryConvertTo<long>(rowValue);
        //                            break;
        //                        case SqlDbType.Bit:
        //                            if (columnType != typeof(bool))
        //                                rowValue = SyncTypeConverter.TryConvertTo<bool>(rowValue);
        //                            break;
        //                        case SqlDbType.Date:
        //                        case SqlDbType.DateTime:
        //                        case SqlDbType.DateTime2:
        //                        case SqlDbType.SmallDateTime:
        //                            if (columnType != typeof(DateTime))
        //                                rowValue = SyncTypeConverter.TryConvertTo<DateTime>(rowValue);
        //                            if (sqlMetadataType == SqlDbType.DateTime && rowValue < SqlDateMin)
        //                                rowValue = SqlDateMin;
        //                            if (sqlMetadataType == SqlDbType.SmallDateTime && rowValue < SqlSmallDateMin)
        //                                rowValue = SqlSmallDateMin;
        //                            break;
        //                        case SqlDbType.DateTimeOffset:
        //                            if (columnType != typeof(DateTimeOffset))
        //                                rowValue = SyncTypeConverter.TryConvertTo<DateTimeOffset>(rowValue);
        //                            break;
        //                        case SqlDbType.Decimal:
        //                            if (columnType != typeof(decimal))
        //                                rowValue = SyncTypeConverter.TryConvertTo<decimal>(rowValue);
        //                            break;
        //                        case SqlDbType.Float:
        //                            if (columnType != typeof(double))
        //                                rowValue = SyncTypeConverter.TryConvertTo<double>(rowValue);
        //                            break;
        //                        case SqlDbType.Real:
        //                            if (columnType != typeof(float))
        //                                rowValue = SyncTypeConverter.TryConvertTo<float>(rowValue);
        //                            break;
        //                        case SqlDbType.Image:
        //                        case SqlDbType.Binary:
        //                        case SqlDbType.VarBinary:
        //                            if (columnType != typeof(byte[]))
        //                                rowValue = SyncTypeConverter.TryConvertTo<byte[]>(rowValue);
        //                            break;
        //                        case SqlDbType.Variant:
        //                            break;
        //                        case SqlDbType.Int:
        //                            if (columnType != typeof(int))
        //                                rowValue = SyncTypeConverter.TryConvertTo<int>(rowValue);
        //                            break;
        //                        case SqlDbType.Money:
        //                        case SqlDbType.SmallMoney:
        //                            if (columnType != typeof(decimal))
        //                                rowValue = SyncTypeConverter.TryConvertTo<decimal>(rowValue);
        //                            break;
        //                        case SqlDbType.NChar:
        //                        case SqlDbType.NText:
        //                        case SqlDbType.VarChar:
        //                        case SqlDbType.Xml:
        //                        case SqlDbType.NVarChar:
        //                        case SqlDbType.Text:
        //                        case SqlDbType.Char:
        //                            if (columnType != typeof(string))
        //                                rowValue = SyncTypeConverter.TryConvertTo<string>(rowValue);
        //                            break;
        //                        case SqlDbType.SmallInt:
        //                            if (columnType != typeof(short))
        //                                rowValue = SyncTypeConverter.TryConvertTo<short>(rowValue);
        //                            break;
        //                        case SqlDbType.Time:
        //                            if (columnType != typeof(TimeSpan))
        //                                rowValue = SyncTypeConverter.TryConvertTo<TimeSpan>(rowValue);
        //                            break;
        //                        case SqlDbType.Timestamp:
        //                            break;
        //                        case SqlDbType.TinyInt:
        //                            if (columnType != typeof(byte))
        //                                rowValue = SyncTypeConverter.TryConvertTo<byte>(rowValue);
        //                            break;
        //                        case SqlDbType.Udt:
        //                            throw new ArgumentException($"Can't use UDT as SQL Type");
        //                        case SqlDbType.UniqueIdentifier:
        //                            if (columnType != typeof(Guid))
        //                                rowValue = SyncTypeConverter.TryConvertTo<Guid>(rowValue);
        //                            break;
        //                    }
        //                }

        //                if (rowValue == null)
        //                    rowValue = DBNull.Value;

        //                record.SetValue(sqlMetadataIndex, rowValue);
        //                sqlMetadataIndex++;
        //            }

        //            records.Add(record);
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        throw new InvalidOperationException($"Can't create a SqlRecord based on the rows we have: {ex.Message}");
        //    }

        //    var sqlParameters = cmd.Parameters as NpgsqlParameterCollection;

        //    sqlParameters["@changeTable"].DataTypeName = string.Empty;
        //    sqlParameters["@changeTable"].Value = records;

        //    if (sqlParameters.Contains("@sync_min_timestamp"))
        //        sqlParameters["@sync_min_timestamp"].Value = lastTimestamp.HasValue ? (object)lastTimestamp.Value : DBNull.Value;

        //    if (sqlParameters.Contains("@sync_scope_id"))
        //        sqlParameters["@sync_scope_id"].Value = senderScopeId;

        //    bool alreadyOpened = connection.State == ConnectionState.Open;

        //    try
        //    {
        //        if (!alreadyOpened)
        //            await connection.OpenAsync().ConfigureAwait(false);

        //        cmd.Transaction = transaction;

        //        using var dataReader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);

        //        while (dataReader.Read())
        //        {
        //            //var itemArray = new object[dataReader.FieldCount];
        //            //var itemArray = new object[failedRows.Columns.Count];
        //            var itemArray = new SyncRow(schemaChangesTable, syncRowState);
        //            for (var i = 0; i < dataReader.FieldCount; i++)
        //            {
        //                var columnValueObject = dataReader.GetValue(i);
        //                var columnName = dataReader.GetName(i);

        //                var columnValue = columnValueObject == DBNull.Value ? null : columnValueObject;

        //                var failedColumn = failedRows.Columns[columnName];
        //                var failedIndexColumn = failedRows.Columns.IndexOf(failedColumn);
        //                itemArray[failedIndexColumn] = columnValue;
        //            }

        //            // don't care about row state 
        //            // Since it will be requested by next request from GetConflict()
        //            failedRows.Rows.Add(itemArray);
        //        }

        //        dataReader.Close();

        //    }
        //    catch (DbException ex)
        //    {
        //        Debug.WriteLine(ex.Message);
        //        throw;
        //    }
        //    finally
        //    {
        //        records.Clear();

        //        if (!alreadyOpened && connection.State != ConnectionState.Closed)
        //            connection.Close();

        //    }
        //}
        //private SqlMetaData GetSqlMetadaFromType(SyncColumn column)
        //{

        //    long maxLength = column.MaxLength;
        //    var dataType = column.GetDataType();
        //    var dbType = column.GetDbType();

        //    var sqlDbType = this.TableDescription.OriginalProvider == NpgsqlSyncProvider.ProviderType ?
        //        this.SqlMetadata.GetNpgsqlDbType(column) : this.SqlMetadata.GetOwnerDbTypeFromDbType(column);

        //    // Since we validate length before, it's not mandatory here.
        //    // let's say.. just in case..
        //    if (sqlDbType == NpgsqlDbType.Text || sqlDbType == NpgsqlDbType.Varchar)
        //    {
        //        // set value for (MAX) 
        //        maxLength = maxLength <= 0 ? DbMetaDataCollectionNames.MetaDataCollections. SqlMetaData.Max : maxLength;

        //        // If max length is specified (not (MAX) )
        //        if (maxLength > 0)
        //            maxLength = sqlDbType == NpgsqlDbType.Varchar ? Math.Min(maxLength, 4000) : Math.Min(maxLength, 8000);

        //        return new SqlMetaData(column.ColumnName, sqlDbType, maxLength);
        //    }


        //    if (dataType == typeof(char))
        //        return new SqlMetaData(column.ColumnName, sqlDbType, 1);

        //    if (sqlDbType == NpgsqlDbType.Char)
        //    {
        //        maxLength = maxLength <= 0 ? (sqlDbType == NpgsqlDbType.Char ? 4000 : 8000) : maxLength;
        //        return new SqlMetaData(column.ColumnName, sqlDbType, maxLength);
        //    }



        //    if (sqlDbType == NpgsqlDbType.Numeric)
        //    {
        //        var (p, s) = this.SqlMetadata.GetPrecisionAndScale(column);
        //        if (p > 0 && p > s)
        //        {
        //            return new SqlMetaData(column.ColumnName, sqlDbType, p, s);
        //        }
        //        else
        //        {
        //            if (p == 0)
        //                p = 18;
        //            if (s == 0)
        //                s = Math.Min((byte)(p - 1), (byte)6);
        //            return new SqlMetaData(column.ColumnName, sqlDbType, p, s);
        //        }
        //    }

        //    return new SqlMetaData(column.ColumnName, sqlDbType);
        //}
        public override (DbCommand Command, bool IsBatchCommand) GetCommand(DbCommandType nameType, SyncFilter filter = null)
        {
            var command = new NpgsqlCommand();
            bool isBatch;
            switch (nameType)
            {
                case DbCommandType.SelectChanges:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectChanges, filter);
                    isBatch = false;
                    break;
                case DbCommandType.SelectInitializedChanges:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectInitializedChanges, filter);
                    isBatch = false;
                    break;
                case DbCommandType.SelectInitializedChangesWithFilters:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectInitializedChangesWithFilters, filter);
                    isBatch = false;
                    break;
                case DbCommandType.SelectChangesWithFilters:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectChangesWithFilters, filter);
                    isBatch = false;
                    break;
                case DbCommandType.SelectRow:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectRow, filter);
                    isBatch = false;
                    break;
                case DbCommandType.UpdateRow:
                case DbCommandType.InsertRow:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.UpdateRow, filter);
                    isBatch = false;
                    break;
                case DbCommandType.UpdateRows:
                case DbCommandType.InsertRows:
                    command.CommandType = CommandType.StoredProcedure;
                    if (this.UseBulkOperations)
                    {
                        command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkUpdateRows, filter);
                        isBatch = true;
                    }
                    else
                    {
                        command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.UpdateRow, filter);
                        isBatch = false;
                    }
                    break;
                case DbCommandType.DeleteRow:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.DeleteRow, filter);
                    isBatch = false;
                    break;
                case DbCommandType.DeleteRows:
                    command.CommandType = CommandType.StoredProcedure;
                    if (this.UseBulkOperations)
                    {
                        command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkDeleteRows, filter);
                        isBatch = true;
                    }
                    else
                    {
                        command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.DeleteRow, filter);
                        isBatch = false;
                    }
                    break;
                case DbCommandType.DisableConstraints:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.NpgsqlObjectNames.GetCommandName(DbCommandType.DisableConstraints, filter);
                    isBatch = false;
                    break;
                case DbCommandType.EnableConstraints:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.NpgsqlObjectNames.GetCommandName(DbCommandType.EnableConstraints, filter);
                    isBatch = false;
                    break;
                case DbCommandType.DeleteMetadata:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.DeleteMetadata, filter);
                    isBatch = false;
                    break;
                case DbCommandType.UpdateMetadata:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.NpgsqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata, filter);
                    isBatch = false;
                    break;
                case DbCommandType.SelectMetadata:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.NpgsqlObjectNames.GetCommandName(DbCommandType.SelectMetadata, filter);
                    isBatch = false;
                    break;
                case DbCommandType.InsertTrigger:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.NpgsqlObjectNames.GetTriggerCommandName(DbTriggerType.Insert, filter);
                    isBatch = false;
                    break;
                case DbCommandType.UpdateTrigger:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.NpgsqlObjectNames.GetTriggerCommandName(DbTriggerType.Update, filter);
                    isBatch = false;
                    break;
                case DbCommandType.DeleteTrigger:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.NpgsqlObjectNames.GetTriggerCommandName(DbTriggerType.Delete, filter);
                    isBatch = false;
                    break;
                case DbCommandType.BulkTableType:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkTableType, filter);
                    isBatch = false;
                    break;
                case DbCommandType.UpdateUntrackedRows:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.NpgsqlObjectNames.GetCommandName(DbCommandType.UpdateUntrackedRows, filter);
                    isBatch = false;
                    break;
                case DbCommandType.Reset:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.Reset, filter);
                    isBatch = false;
                    break;

                default:
                    throw new NotImplementedException($"This command type {nameType} is not implemented");
            }

            return (command, isBatch);
        }

        private void SetSelectChangesParameters(DbCommand command, DbCommandType commandType, SyncFilter filter)
        {
            var originalProvider = NpgsqlSyncProvider.ProviderType;

            var p = command.CreateParameter();
            p.ParameterName = "sync_min_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            if (commandType == DbCommandType.SelectChanges || commandType == DbCommandType.SelectChangesWithFilters)
            {
                p = command.CreateParameter();
                p.ParameterName = "sync_scope_id";
                p.DbType = DbType.Guid;
                command.Parameters.Add(p);
            }

            if (filter == null)
                return;

            var parameters = filter.Parameters;

            if (parameters.Count == 0)
                return;

            foreach (var param in parameters)
            {
                if (param.DbType.HasValue)
                {
                    // Get column name and type
                    var columnName = ParserName.Parse(param.Name).Unquoted().Normalized().ToString();
                    var syncColumn = new SyncColumn(columnName)
                    {
                        DbType = (int)param.DbType.Value,
                        MaxLength = param.MaxLength,
                    };
                    var sqlDbType = this.SqlMetadata.GetOwnerDbTypeFromDbType(syncColumn);

                    var customParameterFilter = new NpgsqlParameter($"@{columnName}", sqlDbType);
                    customParameterFilter.Size = param.MaxLength;
                    customParameterFilter.IsNullable = param.AllowNull;
                    customParameterFilter.Value = param.DefaultValue;

                    command.Parameters.Add(customParameterFilter);
                }
                else
                {
                    var tableFilter = this.TableDescription.Schema.Tables[param.TableName, param.SchemaName];
                    if (tableFilter == null)
                        throw new FilterParamTableNotExistsException(param.TableName);

                    var columnFilter = tableFilter.Columns[param.Name];
                    if (columnFilter == null)
                        throw new FilterParamColumnNotExistsException(param.Name, param.TableName);

                    // Get column name and type
                    var columnName = ParserName.Parse(columnFilter).Normalized().Unquoted().ToString();

                    var sqlDbType = tableFilter.OriginalProvider == originalProvider ?
                        this.SqlMetadata.GetNpgsqlDbType(columnFilter) : this.SqlMetadata.GetOwnerDbTypeFromDbType(columnFilter);

                    // Add it as parameter
                    var sqlParamFilter = new NpgsqlParameter($"@{columnName}", sqlDbType);
                    sqlParamFilter.Size = columnFilter.MaxLength;
                    sqlParamFilter.IsNullable = param.AllowNull;
                    sqlParamFilter.Value = param.DefaultValue;
                    command.Parameters.Add(sqlParamFilter);
                }
            }
        }

        private void SetSelectRowParameters(DbCommand command)
        {
            DbParameter p;

            foreach (var column in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var unquotedColumn = ParserName.Parse(column).Normalized().Unquoted().ToString();
                p = command.CreateParameter();
                p.ParameterName = $"@{unquotedColumn}";
                p.DbType = column.GetDbType();
                p.SourceColumn = column.ColumnName;
                p.Size = column.MaxLength;
                command.Parameters.Add(p);
            }
        }

        private void SetUpdateRowParameters(DbCommand command)
        {
            DbParameter p;

            foreach (var column in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var unquotedColumn = ParserName.Parse(column).Normalized().Unquoted().ToString();
                p = command.CreateParameter();
                p.ParameterName = $"@{unquotedColumn}";
                p.DbType = column.GetDbType();
                p.SourceColumn = column.ColumnName;
                p.Size = column.MaxLength;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            p.Size = 32;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_row_is_tombstone";
            p.DbType = DbType.Boolean;
            p.Size = 2;
            command.Parameters.Add(p);
        }

    }
}
