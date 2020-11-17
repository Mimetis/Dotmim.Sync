using Dotmim.Sync.Builders;
using Dotmim.Sync.SqlServer.Manager;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient.Server;
using System.Text;
using Dotmim.Sync.Manager;
using System.IO;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlSyncAdapter : SyncAdapter
    {
        private SqlObjectNames sqlObjectNames;
        private SqlDbMetadata sqlMetadata;

        // Derive Parameters cache
        // Be careful, we can have collision between databases
        // this static class could be shared accross databases with same command name
        // but different table schema
        // So the string should contains the connection string as well
        private static ConcurrentDictionary<string, List<SqlParameter>> derivingParameters
            = new ConcurrentDictionary<string, List<SqlParameter>>();


        // Internal commands cache
        private ConcurrentDictionary<string, Lazy<DbCommand>> commands = new ConcurrentDictionary<string, Lazy<DbCommand>>();

        private readonly ParserName tableName;
        private readonly ParserName trackingName;
        private readonly SyncSetup setup;
        private readonly SqlBuilderCommands sqlBuilderCommands;

        public SqlSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup) 
            : base(tableDescription, setup)
        {
            this.sqlMetadata = new SqlDbMetadata();
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.setup = setup;
            this.sqlBuilderCommands = new SqlBuilderCommands(tableDescription, tableName, trackingName, setup);
            this.sqlObjectNames = new SqlObjectNames(tableDescription, setup);
        }

        private SqlMetaData GetSqlMetadaFromType(SyncColumn column)
        {
            long maxLength = column.MaxLength;
            var dataType = column.GetDataType();
            var dbType = column.GetDbType();

            var sqlDbType = (SqlDbType)this.sqlMetadata.TryGetOwnerDbType(column.OriginalDbType, dbType, false, false, column.MaxLength, this.TableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

            // TODO : Since we validate length before, is it mandatory here ?

            if (sqlDbType == SqlDbType.VarChar || sqlDbType == SqlDbType.NVarChar)
            {
                // set value for (MAX) 
                maxLength = maxLength < 0 ? SqlMetaData.Max : maxLength;

                // If max length is specified (not (MAX) )
                if (maxLength > 0)
                    maxLength = sqlDbType == SqlDbType.NVarChar ? Math.Min(maxLength, 4000) : Math.Min(maxLength, 8000);

                return new SqlMetaData(column.ColumnName, sqlDbType, maxLength);
            }


            if (dataType == typeof(char))
                return new SqlMetaData(column.ColumnName, sqlDbType, 1);

            if (sqlDbType == SqlDbType.Char || sqlDbType == SqlDbType.NChar)
            {
                maxLength = maxLength <= 0 ? (sqlDbType == SqlDbType.NChar ? 4000 : 8000) : maxLength;
                return new SqlMetaData(column.ColumnName, sqlDbType, maxLength);
            }

            if (sqlDbType == SqlDbType.Binary)
            {
                maxLength = maxLength <= 0 ? 8000 : maxLength;
                return new SqlMetaData(column.ColumnName, sqlDbType, maxLength);
            }

            if (sqlDbType == SqlDbType.VarBinary)
            {
                // set value for (MAX) 
                maxLength = maxLength <= 0 ? SqlMetaData.Max : maxLength;

                return new SqlMetaData(column.ColumnName, sqlDbType, maxLength);
            }

            if (sqlDbType == SqlDbType.Decimal)
            {
                if (column.PrecisionSpecified && column.ScaleSpecified)
                {
                    var (p, s) = this.sqlMetadata.ValidatePrecisionAndScale(column);
                    return new SqlMetaData(column.ColumnName, sqlDbType, p, s);

                }

                if (column.PrecisionSpecified)
                {
                    var p = this.sqlMetadata.ValidatePrecision(column);
                    return new SqlMetaData(column.ColumnName, sqlDbType, p);
                }

            }

            return new SqlMetaData(column.ColumnName, sqlDbType);

        }

        public override async Task<string> PreExecuteBatchCommandAsync(DbCommandType commandType, Guid senderScopeId,
                                                                 SyncTable schemaChangesTable, SyncTable failedRows,
                                                                 long lastTimestamp,
                                                                 DbConnection connection, DbTransaction transaction)
        {
            //if (commandType == DbCommandType.UpdateBatchRows)
            //    return await PreExecuteBatchUpdateWithTmpCommandAsync(commandType, senderScopeId, schemaChangesTable, failedRows, lastTimestamp, connection, transaction);


            return null;
        }

        public override async Task PostExecuteBatchCommandAsync(DbCommandType commandType, Guid senderScopeId,
                                                                 SyncTable schemaChangesTable, SyncTable failedRows,
                                                                 long lastTimestamp, string optionalState,
                                                                 DbConnection connection, DbTransaction transaction)
        {
            //if (commandType == DbCommandType.UpdateBatchRows)
            //    await PostExecuteBatchUpdateWithTmpCommand(commandType, senderScopeId, schemaChangesTable, failedRows, lastTimestamp, optionalState, connection, transaction);
        }

        public async override Task ExecuteBatchCommandAsync(DbCommandType commandType, Guid senderScopeId, IEnumerable<SyncRow> applyRows, SyncTable schemaChangesTable,
                                                            SyncTable failedRows, long lastTimestamp, string optionalState, DbConnection connection, DbTransaction transaction = null)
        {

            if (commandType == DbCommandType.UpdateBatchRows)
                await ExecuteBatchUpdateWithTvpCommand(commandType, senderScopeId, applyRows, schemaChangesTable, failedRows, lastTimestamp, optionalState, connection, transaction);
            // await ExecuteBatchUpdateWithTmpCommand(senderScopeId, applyRows, schemaChangesTable, failedRows, lastTimestamp, optionalState, connection, transaction);
        }


        public override Task<string> PreExecuteCommandAsync(DbCommandType commandType, DbConnection connection, DbTransaction transaction)
            => Task.FromResult(string.Empty);

        public override Task PostExecuteCommandAsync(DbCommandType commandType, string optionalState, DbConnection connection, DbTransaction transaction)
            => Task.CompletedTask;



        internal void SetColumnParametersValues(DbCommand command, SyncRow row)
        {
            if (row.Table == null)
                throw new ArgumentException("Schema table columns does not correspond to row values");

            var schemaTable = row.Table;

            foreach (DbParameter parameter in command.Parameters)
            {
                // foreach parameter, check if we have a column 
                if (!string.IsNullOrEmpty(parameter.SourceColumn))
                {
                    var column = schemaTable.Columns.FirstOrDefault(sc => sc.ColumnName.Equals(parameter.SourceColumn, SyncGlobalization.DataSourceStringComparison));
                    if (column != null)
                    {
                        object value = row[column] ?? DBNull.Value;
                        DbTableManagerFactory.SetParameterValue(command, parameter.ParameterName, value);
                    }
                }

            }

            // return value
            var syncRowCountParam = DbTableManagerFactory.GetParameter(command, "sync_row_count");

            if (syncRowCountParam != null)
                syncRowCountParam.Direction = ParameterDirection.Output;
        }

        /// <summary>
        /// Check if an exception is a primary key exception
        /// </summary>
        public override bool IsPrimaryKeyViolation(Exception exception)
        {
            if (exception is SqlException error && error.Number == 2627)
                return true;

            return false;
        }


        public override bool IsUniqueKeyViolation(Exception exception)
        {
            if (exception is SqlException error && error.Number == 2627)
                return true;

            return false;
        }

        public override DbCommand GetCommand(DbCommandType commandType, SyncFilter filter)
        {
            var command = new SqlCommand();

            string commandText = null;

            switch (commandType)
            {
                case DbCommandType.SelectChanges:
                    commandText = this.sqlBuilderCommands.GetSelectChangesCommandText();
                    break;
                case DbCommandType.SelectChangesWithFilters:
                    commandText = this.sqlBuilderCommands.GetSelectChangesCommandText(filter);
                    break;
                case DbCommandType.SelectInitializedChanges:
                    commandText = this.sqlBuilderCommands.GetSelectInitializeChangesCommandText();
                    break;
                case DbCommandType.SelectInitializedChangesWithFilters:
                    commandText = this.sqlBuilderCommands.GetSelectInitializeChangesCommandText(filter);
                    break;
                case DbCommandType.SelectRow:
                    commandText = this.sqlBuilderCommands.GetSelectRowCommandText();
                    break;
                case DbCommandType.UpdateRow:
                    commandText = this.sqlBuilderCommands.GetUpdateCommandText();
                    break;
                case DbCommandType.UpdateBatchRows:
                    commandText = this.sqlBuilderCommands.GetUpdateBulkCommandWithTvpText();
                    break;
                case DbCommandType.DeleteRow:
                    commandText = this.sqlBuilderCommands.GetDeleteRowCommandText();
                    break;
                case DbCommandType.DeleteBatchRows:
                    commandText = this.sqlBuilderCommands.GetDeleteBulkCommandText();
                    break;
                case DbCommandType.DisableConstraints:
                    commandText = this.sqlBuilderCommands.GetDisableConstraintsCommandText();
                    break;
                case DbCommandType.EnableConstraints:
                    commandText = this.sqlBuilderCommands.GetEnableConstraintsCommandText();
                    break;
                case DbCommandType.DeleteMetadata:
                    commandText = this.sqlBuilderCommands.GetDeleteMetadataRowCommandText();
                    break;
                case DbCommandType.UpdateMetadata:
                    commandText = this.sqlBuilderCommands.GetUpdateMetadataRowCommandText();
                    break;
                case DbCommandType.UpdateUntrackedRows:
                    commandText = this.sqlBuilderCommands.GetUpdateUntrackedRowsCommandText();
                    break;
                case DbCommandType.Reset:
                    var ins = this.sqlObjectNames.GetInsertTriggerName();
                    var upd = this.sqlObjectNames.GetUpdateTriggerName();
                    var del = this.sqlObjectNames.GetDeleteTriggerName();
                    commandText = this.sqlBuilderCommands.GetResetCommandText(ins, upd, del);
                    break;
                default:
                    break;
            }

            command.CommandType = CommandType.Text;
            command.CommandText = commandText;

            return command;
        }

        /// <summary>
        /// Set a stored procedure parameters or text parameters
        /// </summary>
        public override Task AddCommandParametersAsync(DbCommandType commandType, DbCommand command, DbConnection connection, DbTransaction transaction = null, SyncFilter filter = null)
        {
            if (command == null)
                return Task.CompletedTask;

            if (command.Parameters != null && command.Parameters.Count > 0)
                return Task.CompletedTask;


            switch (commandType)
            {
                case DbCommandType.SelectChanges:
                    this.SetSelecteChangesParameters(command);
                    break;
                case DbCommandType.SelectChangesWithFilters:
                    this.SetSelecteChangesWithFiltersParameters(command, filter);
                    break;
                case DbCommandType.SelectInitializedChanges:
                    // No parameters for this one
                    break;
                case DbCommandType.SelectInitializedChangesWithFilters:
                    this.SetSelecteInitializeChangesWithFiltersParameters(command, filter);
                    break;
                case DbCommandType.SelectRow:
                    this.SetSelectRowParameters(command);
                    break;
                case DbCommandType.UpdateRow:
                    this.SetUpdateRowParameters(command);
                    break;
                case DbCommandType.UpdateBatchRows:
                    this.SetUpdateBatchRowsParameters(command);
                    break;
                case DbCommandType.DeleteRow:
                    this.SetDeleteRowParameters(command);
                    break;
                case DbCommandType.DeleteBatchRows:
                    this.SetDeleteBatchRowsParameters(command);
                    break;
                case DbCommandType.DisableConstraints:
                    // No parameters
                    break;
                case DbCommandType.EnableConstraints:
                    // No parameters
                    break;
                case DbCommandType.DeleteMetadata:
                    this.SetDeleteMetadataParameters(command);
                    break;
                case DbCommandType.UpdateMetadata:
                    this.SetUpdateMetadataParameters(command);
                    break;
                case DbCommandType.UpdateUntrackedRows:
                    break;
                case DbCommandType.Reset:
                    // No parameters
                    break;
                default:
                    break;
            }

            return Task.CompletedTask;

        }


        /// <summary>
        /// Execute batch update when used with TVP
        /// </summary>
        private async Task ExecuteBatchUpdateWithTmpCommand(Guid senderScopeId, IEnumerable<SyncRow> applyRows, SyncTable schemaChangesTable,
                                                            SyncTable failedRows, long lastTimestamp, string optionalState, DbConnection connection, DbTransaction transaction = null)
        {
            var applyRowsCount = applyRows.Count();

            if (applyRowsCount <= 0)
                return;

            bool alreadyOpened = connection.State == ConnectionState.Open;


            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);


            // Create the key
            var commandKey = $"{connection.DataSource}-{connection.Database}-{this.TableDescription.GetFullName()}-{DbCommandType.UpdateBatchRows}-{applyRowsCount}";

            var commandExist = commands.TryGetValue(commandKey, out var lazyCommand);

            SqlCommand commandTmpInsert = null;

            if (commandExist)
            {
                commandTmpInsert = lazyCommand.Value as SqlCommand;
            }
            else
            {
                commandTmpInsert = new SqlCommand();
            }

            commandTmpInsert.Connection = connection as SqlConnection;

            if (transaction != null)
                commandTmpInsert.Transaction = transaction as SqlTransaction;

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"Insert into {optionalState} Values ");

            int cpt = 0;
            string commaLine = "";
            foreach (var row in applyRows)
            {
                stringBuilder.Append($"{commaLine}(");
                string comma = "";
                foreach (var column in row.Table.Columns.Where(c => !c.IsReadOnly))
                {
                    // build parameter command
                    stringBuilder.Append($"{comma}@P{cpt}");

                    long maxLength = column.MaxLength;
                    var dataType = column.GetDataType();
                    var dbType = column.GetDbType();

                    var sqlDbType = (SqlDbType)this.sqlMetadata.TryGetOwnerDbType(column.OriginalDbType, dbType, false, false, column.MaxLength, this.TableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

                    if (!commandExist)
                    {

                        // add parameter
                        var p = new SqlParameter
                        {
                            ParameterName = $"@P{cpt}",
                            DbType = column.GetDbType(),
                            Size = column.MaxLength,
                            SourceColumn = column.ColumnName,
                            Value = row[column] == null ? DBNull.Value : row[column]
                        };

                        if (column.PrecisionSpecified && column.ScaleSpecified)
                        {
                            var (precision, scale) = this.sqlMetadata.ValidatePrecisionAndScale(column);
                            p.Precision = precision;
                            p.Scale = scale;

                        }

                        if (column.PrecisionSpecified)
                        {
                            var precision = this.sqlMetadata.ValidatePrecision(column);
                            p.Precision = precision;
                            p.Scale = 0;
                        }

                        commandTmpInsert.Parameters.Add(p);
                    }
                    else
                    {
                        commandTmpInsert.Parameters[$"@P{cpt}"].Value = row[column] == null ? DBNull.Value : row[column];

                    }

                    cpt++;
                    comma = ", ";
                }

                stringBuilder.AppendLine(")");
                commaLine = ", ";
            }

            if (!commandExist)
            {
                var commandTmpInsertText = stringBuilder.ToString();
                commandTmpInsert.CommandText = commandTmpInsertText;
                commandTmpInsert.Prepare();

                // Adding this command as prepared
                //lazyCommand.Value.IsPrepared = true;

                commands.AddOrUpdate(commandKey, new Lazy<DbCommand>(() => commandTmpInsert)
                                               , (key, lc) => new Lazy<DbCommand>(() => lc.Value));

            }

            await commandTmpInsert.ExecuteNonQueryAsync().ConfigureAwait(false);



        }



        public async Task<string> PreExecuteBatchUpdateWithTmpCommandAsync(DbCommandType commandType, Guid senderScopeId,
                                                         SyncTable schemaChangesTable, SyncTable failedRows,
                                                         long lastTimestamp,
                                                         DbConnection connection, DbTransaction transaction)
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;
            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                var randomString = Path.GetRandomFileName().Replace(".", "");
                var tableName = this.tableName.Unquoted().Normalized().ToString();
                var tmptTableName = $"#{tableName}_{randomString}";

                // Create the TVP name
                using var commandCreateTableTmp = connection.CreateCommand();

                commandCreateTableTmp.Connection = connection;
                commandCreateTableTmp.CommandText = this.sqlBuilderCommands.GetTempTableCommandText(tmptTableName);

                if (transaction != null)
                    commandCreateTableTmp.Transaction = transaction;

                commandCreateTableTmp.ExecuteNonQuery();

                return tmptTableName;
            }
            catch (DbException ex)
            {
                Debug.WriteLine(ex.Message);
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();
            }
        }

        private async Task PostExecuteBatchUpdateWithTmpCommand(DbCommandType commandType, Guid senderScopeId,
                                                                 SyncTable schemaChangesTable, SyncTable failedRows,
                                                                 long lastTimestamp, string optionalState,
                                                                 DbConnection connection, DbTransaction transaction)
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;
            try
            {
                // Now merge
                using var commandMerge = new SqlCommand()
                {
                    Connection = connection as SqlConnection
                };

                if (transaction != null)
                    commandMerge.Transaction = transaction as SqlTransaction;

                commandMerge.CommandText = this.sqlBuilderCommands.GetUpdateBulkCommandWithTempTableText(optionalState);

                await this.AddCommandParametersAsync(DbCommandType.UpdateBatchRows, commandMerge, connection, transaction);

                ((SqlParameterCollection)commandMerge.Parameters)["@sync_min_timestamp"].Value = lastTimestamp;
                ((SqlParameterCollection)commandMerge.Parameters)["@sync_scope_id"].Value = senderScopeId;


                using var dataReader = await commandMerge.ExecuteReaderAsync().ConfigureAwait(false);

                while (dataReader.Read())
                {
                    //var itemArray = new object[dataReader.FieldCount];
                    var itemArray = new object[failedRows.Columns.Count];
                    for (var i = 0; i < dataReader.FieldCount; i++)
                    {
                        var columnValueObject = dataReader.GetValue(i);
                        var columnName = dataReader.GetName(i);

                        var columnValue = columnValueObject == DBNull.Value ? null : columnValueObject;

                        var failedColumn = failedRows.Columns[columnName];
                        var failedIndexColumn = failedRows.Columns.IndexOf(failedColumn);
                        itemArray[failedIndexColumn] = columnValue;
                    }

                    // don't care about row state 
                    // Since it will be requested by next request from GetConflict()
                    failedRows.Rows.Add(itemArray, base.ApplyType);
                }
            }
            catch (DbException ex)
            {
                Debug.WriteLine(ex.Message);
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();
            }

        }

        /// <summary>
        /// Execute batch update when used with TVP
        /// </summary>
        private async Task ExecuteBatchUpdateWithTvpCommand(DbCommandType commandType, Guid senderScopeId, IEnumerable<SyncRow> applyRows, SyncTable schemaChangesTable,
                                                            SyncTable failedRows, long lastTimestamp, string optionalState, DbConnection connection, DbTransaction transaction = null)
        {
            var applyRowsCount = applyRows.Count();

            if (applyRowsCount <= 0)
                return;

            var dataRowState = DataRowState.Unchanged;

            var records = new List<SqlDataRecord>(applyRowsCount);
            SqlMetaData[] metadatas = new SqlMetaData[schemaChangesTable.Columns.Count];

            for (int i = 0; i < schemaChangesTable.Columns.Count; i++)
                metadatas[i] = GetSqlMetadaFromType(schemaChangesTable.Columns[i]);

            try
            {
                foreach (var row in applyRows)
                {
                    dataRowState = row.RowState;

                    var record = new SqlDataRecord(metadatas);

                    int sqlMetadataIndex = 0;

                    for (int i = 0; i < schemaChangesTable.Columns.Count; i++)
                    {
                        var schemaColumn = schemaChangesTable.Columns[i];

                        // Get the default value
                        //var columnType = schemaColumn.GetDataType();
                        dynamic defaultValue = schemaColumn.GetDefaultValue();
                        dynamic rowValue = row[i];

                        // metadatas don't have readonly values, so get from sqlMetadataIndex
                        var sqlMetadataType = metadatas[sqlMetadataIndex].SqlDbType;

                        if (rowValue != null)
                        {
                            var columnType = rowValue.GetType();

                            switch (sqlMetadataType)
                            {
                                case SqlDbType.BigInt:
                                    if (columnType != typeof(long))
                                        rowValue = SyncTypeConverter.TryConvertTo<long>(rowValue);
                                    break;
                                case SqlDbType.Bit:
                                    if (columnType != typeof(bool))
                                        rowValue = SyncTypeConverter.TryConvertTo<bool>(rowValue);
                                    break;
                                case SqlDbType.Date:
                                case SqlDbType.DateTime:
                                case SqlDbType.DateTime2:
                                case SqlDbType.SmallDateTime:
                                    if (columnType != typeof(DateTime))
                                        rowValue = SyncTypeConverter.TryConvertTo<DateTime>(rowValue);
                                    break;
                                case SqlDbType.DateTimeOffset:
                                    if (columnType != typeof(DateTimeOffset))
                                        rowValue = SyncTypeConverter.TryConvertTo<DateTimeOffset>(rowValue);
                                    break;
                                case SqlDbType.Decimal:
                                    if (columnType != typeof(decimal))
                                        rowValue = SyncTypeConverter.TryConvertTo<decimal>(rowValue);
                                    break;
                                case SqlDbType.Float:
                                    if (columnType != typeof(double))
                                        rowValue = SyncTypeConverter.TryConvertTo<double>(rowValue);
                                    break;
                                case SqlDbType.Real:
                                    if (columnType != typeof(float))
                                        rowValue = SyncTypeConverter.TryConvertTo<float>(rowValue);
                                    break;
                                case SqlDbType.Image:
                                case SqlDbType.Binary:
                                case SqlDbType.VarBinary:
                                    if (columnType != typeof(byte[]))
                                        rowValue = SyncTypeConverter.TryConvertTo<byte[]>(rowValue);
                                    break;
                                case SqlDbType.Variant:
                                    break;
                                case SqlDbType.Int:
                                    if (columnType != typeof(int))
                                        rowValue = SyncTypeConverter.TryConvertTo<int>(rowValue);
                                    break;
                                case SqlDbType.Money:
                                case SqlDbType.SmallMoney:
                                    if (columnType != typeof(decimal))
                                        rowValue = SyncTypeConverter.TryConvertTo<decimal>(rowValue);
                                    break;
                                case SqlDbType.NChar:
                                case SqlDbType.NText:
                                case SqlDbType.VarChar:
                                case SqlDbType.Xml:
                                case SqlDbType.NVarChar:
                                case SqlDbType.Text:
                                case SqlDbType.Char:
                                    if (columnType != typeof(string))
                                        rowValue = SyncTypeConverter.TryConvertTo<string>(rowValue);
                                    break;
                                case SqlDbType.SmallInt:
                                    if (columnType != typeof(short))
                                        rowValue = SyncTypeConverter.TryConvertTo<short>(rowValue);
                                    break;
                                case SqlDbType.Time:
                                    if (columnType != typeof(TimeSpan))
                                        rowValue = SyncTypeConverter.TryConvertTo<TimeSpan>(rowValue);
                                    break;
                                case SqlDbType.Timestamp:
                                    break;
                                case SqlDbType.TinyInt:
                                    if (columnType != typeof(byte))
                                        rowValue = SyncTypeConverter.TryConvertTo<byte>(rowValue);
                                    break;
                                case SqlDbType.Udt:
                                    throw new ArgumentException($"Can't use UDT as SQL Type");
                                case SqlDbType.UniqueIdentifier:
                                    if (columnType != typeof(Guid))
                                        rowValue = SyncTypeConverter.TryConvertTo<Guid>(rowValue);
                                    break;
                            }
                        }

                        if (rowValue == null)
                            rowValue = DBNull.Value;

                        record.SetValue(sqlMetadataIndex, rowValue);
                        sqlMetadataIndex++;
                    }


                    records.Add(record);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Can't create a SqlRecord based on the rows we have: {ex.Message}");
            }



            // Create the key
            var commandKey = $"{connection.DataSource}-{connection.Database}-{this.TableDescription.GetFullName()}-{DbCommandType.UpdateBatchRows}-{applyRowsCount}";

            var commandExist = commands.TryGetValue(commandKey, out var lazyCommand);

            SqlCommand cmd = null;

            if (commandExist)
            {
                cmd = lazyCommand.Value as SqlCommand;
            }
            else
            {
                cmd = new SqlCommand();
            }


            cmd.Connection = connection as SqlConnection;

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                if (transaction != null)
                    cmd.Transaction = transaction as SqlTransaction;

                if (!commandExist)
                {
                    cmd.CommandText = this.sqlBuilderCommands.GetUpdateBulkCommandWithTvpText();

                    await this.AddCommandParametersAsync(DbCommandType.UpdateBatchRows, cmd, connection, transaction);

                    var tvpName = this.tableName.Schema().Unquoted().Normalized().ToString();

                    var p = new SqlParameter();
                    p.ParameterName = "@changeTable";
                    p.TypeName = tvpName;
                    p.Size = -1;
                    p.SqlDbType = SqlDbType.Structured;
                    cmd.Parameters.Add(p);

                    cmd.Prepare();

                    commands.AddOrUpdate(commandKey, new Lazy<DbCommand>(() => cmd)
                                                   , (key, lc) => new Lazy<DbCommand>(() => lc.Value));
                }

                cmd.Parameters["@changeTable"].Value = records;
                cmd.Parameters["@sync_min_timestamp"].Value = lastTimestamp;
                cmd.Parameters["@sync_scope_id"].Value = senderScopeId;

                using var dataReader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);

                while (dataReader.Read())
                {
                    //var itemArray = new object[dataReader.FieldCount];
                    var itemArray = new object[failedRows.Columns.Count];
                    for (var i = 0; i < dataReader.FieldCount; i++)
                    {
                        var columnValueObject = dataReader.GetValue(i);
                        var columnName = dataReader.GetName(i);

                        var columnValue = columnValueObject == DBNull.Value ? null : columnValueObject;

                        var failedColumn = failedRows.Columns[columnName];
                        var failedIndexColumn = failedRows.Columns.IndexOf(failedColumn);
                        itemArray[failedIndexColumn] = columnValue;
                    }

                    // don't care about row state 
                    // Since it will be requested by next request from GetConflict()
                    failedRows.Rows.Add(itemArray, dataRowState);
                }
            }
            catch (DbException ex)
            {
                Debug.WriteLine(ex.Message);
                throw;
            }
            finally
            {
                records.Clear();

                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }
        }

        private void SetUpdateBatchRowsParameters(DbCommand command)
        {
            SqlParameter p;

            p = new SqlParameter();
            p.ParameterName = "@sync_min_timestamp";
            p.DbType = DbType.Int64;
            p.Size = 4;
            command.Parameters.Add(p);

            p = new SqlParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            p.Size = 36;
            command.Parameters.Add(p);

            // Not used wit tmp table
            //p = new SqlParameter();
            //p.ParameterName = "@changeTable";
            //p.SqlDbType = SqlDbType.Structured;
            //command.Parameters.Add(p);
        }

        private void SetDeleteBatchRowsParameters(DbCommand command)
        {
            throw new NotImplementedException();
        }

        private void SetSelecteChangesWithFiltersParameters(DbCommand command, SyncFilter filter)
        {
            throw new NotImplementedException();
        }

        private void SetSelecteInitializeChangesWithFiltersParameters(DbCommand command, SyncFilter filter)
        {
            throw new NotImplementedException();
        }

        private void SetUpdateRowParameters(DbCommand command)
        {
            DbParameter p;

            foreach (var column in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var unquotedColumn = ParserName.Parse(column).Normalized().Unquoted().ToString();
                p = command.CreateParameter();
                p.ParameterName = $"@{unquotedColumn}";
                p.DbType = column.GetDbType();
                p.Size = column.MaxLength;
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = "@sync_force_write";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_min_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);
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
                p.Size = column.MaxLength;
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

        }

        private void SetSelecteChangesParameters(DbCommand command)
        {
            var p = command.CreateParameter();
            p.ParameterName = "@sync_min_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);
        }

        private void SetDeleteRowParameters(DbCommand command)
        {
            DbParameter p;

            foreach (var column in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var unquotedColumn = ParserName.Parse(column).Normalized().Unquoted().ToString();
                p = command.CreateParameter();
                p.ParameterName = $"@{unquotedColumn}";
                p.Size = column.MaxLength;
                p.DbType = column.GetDbType();
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = "@sync_force_write";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_min_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

        }

        private void SetDeleteMetadataParameters(DbCommand command)
        {
            var p = command.CreateParameter();
            p.ParameterName = "@sync_row_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);
        }

        private void SetUpdateMetadataParameters(DbCommand command)
        {
            DbParameter p;

            foreach (var column in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var unquotedColumn = ParserName.Parse(column).Normalized().Unquoted().ToString();
                p = command.CreateParameter();
                p.ParameterName = $"@{unquotedColumn}";
                p.Size = column.MaxLength;
                p.DbType = column.GetDbType();
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_row_is_tombstone";
            p.DbType = DbType.Boolean;
            command.Parameters.Add(p);

        }

    }
}
