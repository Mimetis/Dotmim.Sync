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

        public SqlSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup) : base(tableDescription, setup)
        {
            this.sqlObjectNames = new SqlObjectNames(tableDescription, tableName, trackingName, setup);
            this.sqlMetadata = new SqlDbMetadata();
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



        private void BuildTmpInsertCommand(DbCommand command, IEnumerable<SyncRow> rows)
        {
            var stringBuilder = new StringBuilder();
            var sqlDbMetadata = new SqlDbMetadata();

            var tableName = string.Concat("@", this.TableDescription.GetFullName().Replace(".", "_"));

            string str = "";

            stringBuilder.AppendLine($"DECLARE {tableName} dbo.Customer_BulkType");

            stringBuilder.AppendLine($"INSERT INTO {tableName} (");
            foreach (var c in this.TableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"{str}{columnName}");
                str = ", ";
            }
            stringBuilder.Append($") VALUES ");

            int cpt = 0;


            string rowStr = "";
            foreach (var row in rows)
            {
                stringBuilder.Append($"{rowStr}(");

                str = "";
                foreach (var c in this.TableDescription.Columns.Where(col => !col.IsReadOnly))
                {
                    if (row[c] == null || row[c] == DBNull.Value)
                        stringBuilder.Append($"{str}NULL");
                    else if (sqlDbMetadata.IsTextType(c.OriginalDbType))
                        stringBuilder.Append($"{str}N'{row[c]}'");
                    else if (c.GetDbType() == DbType.Guid)
                        stringBuilder.Append($"{str}'{row[c]}'");
                    else
                        stringBuilder.Append($"{str}{row[c]}");


                    //var parameterName = ParserName.Parse(c).Unquoted().Normalized().ToString();
                    //stringBuilder.Append($"{str}@P{cpt}");

                    //var unquotedColumn = ParserName.Parse(c).Normalized().Unquoted().ToString();
                    //var p = command.CreateParameter();
                    //p.ParameterName = $"@P{cpt}";
                    //p.DbType = c.GetDbType();
                    //p.Size = c.MaxLength;
                    //p.SourceColumn = c.ColumnName;
                    //p.Value = SyncTypeConverter.TryConvertFromDbType(row[c], p.DbType);
                    //if (p.Value == null)
                    //    p.Value = DBNull.Value;

                    //command.Parameters.Add(p);

                    str = ", ";
                    cpt++;
                }
                stringBuilder.AppendLine($")");
                rowStr = ",";
            }

            var buildTmpInsertCommand = stringBuilder.ToString();

            command.CommandText += buildTmpInsertCommand;
        }

        private string BuildTempTable()
        {
            StringBuilder stringBuilder = new StringBuilder();
            var sqlDbMetadata = new SqlDbMetadata();

            var tableName = string.Concat("#", this.TableDescription.GetFullName().Replace(".", "_"));

            stringBuilder.AppendLine($"CREATE TABLE {tableName} (");
            string str = "";
            foreach (var c in this.TableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var isPrimaryKey = this.TableDescription.IsPrimaryKey(c.ColumnName);

                var columnName = ParserName.Parse(c).Quoted().ToString();
                var nullString = isPrimaryKey ? "NOT NULL" : "NULL";

                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = sqlDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.GetDbType(), false, false, c.MaxLength, this.TableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var quotedColumnType = ParserName.Parse(sqlDbTypeString).Quoted().ToString();
                quotedColumnType += sqlDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.GetDbType(), false, false, c.MaxLength, c.Precision, c.Scale, this.TableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

                stringBuilder.AppendLine($"{str}{columnName} {quotedColumnType} {nullString}");
                str = ", ";
            }
            //stringBuilder.AppendLine(", [update_scope_id] [uniqueidentifier] NULL");
            stringBuilder.Append(string.Concat(str, "PRIMARY KEY ("));
            str = "";
            foreach (var c in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"{str}{columnName} ASC");
                str = ", ";
            }

            stringBuilder.Append("))");
            return stringBuilder.ToString();
        }
        private string BuildVarTable()
        {
            StringBuilder stringBuilder = new StringBuilder();
            var sqlDbMetadata = new SqlDbMetadata();

            var tableName = string.Concat("@", this.TableDescription.GetFullName().Replace(".", "_"));

            stringBuilder.AppendLine($"DECLARE {tableName} TABLE (");
            string str = "";
            foreach (var c in this.TableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var isPrimaryKey = this.TableDescription.IsPrimaryKey(c.ColumnName);

                var columnName = ParserName.Parse(c).Quoted().ToString();
                var nullString = isPrimaryKey ? "NOT NULL" : "NULL";

                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = sqlDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.GetDbType(), false, false, c.MaxLength, this.TableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var quotedColumnType = ParserName.Parse(sqlDbTypeString).Quoted().ToString();
                quotedColumnType += sqlDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.GetDbType(), false, false, c.MaxLength, c.Precision, c.Scale, this.TableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

                stringBuilder.AppendLine($"{str}{columnName} {quotedColumnType} {nullString}");
                str = ", ";
            }

            //stringBuilder.Append(string.Concat(str, "PRIMARY KEY ("));
            //str = "";
            //foreach (var c in this.TableDescription.GetPrimaryKeysColumns())
            //{
            //    var columnName = ParserName.Parse(c).Quoted().ToString();
            //    stringBuilder.Append($"{str}{columnName} ASC");
            //    str = ", ";
            //}
            //stringBuilder.Append("))");

            stringBuilder.Append(")");
            return stringBuilder.ToString();
        }

        public override async Task BuildTmpTableAsync(DbConnection connection, DbTransaction transaction)
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;
            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                // Create table tmp
                var commandCreateTableTmp = connection.CreateCommand();
                commandCreateTableTmp.Connection = connection;
                commandCreateTableTmp.CommandText = BuildTempTable();
                if (transaction != null)
                    commandCreateTableTmp.Transaction = transaction;

                commandCreateTableTmp.ExecuteNonQuery();

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

        public override async Task ExecuteBatchCommandAsync2(DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> applyRows, SyncTable schemaChangesTable, SyncTable failedRows, long lastTimestamp, DbConnection connection, DbTransaction transaction)
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                var commandInsertIntoTableTmp = connection.CreateCommand();
                commandInsertIntoTableTmp.Connection = connection;

                if (transaction != null)
                    commandInsertIntoTableTmp.Transaction = transaction;

                BuildTmpInsertCommand(commandInsertIntoTableTmp, applyRows);
                commandInsertIntoTableTmp.ExecuteNonQuery();


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
        /// Executing a batch command
        /// </summary>
        public async override Task ExecuteBatchCommandAsync(DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> applyRows, SyncTable schemaChangesTable,
                                                            SyncTable failedRows, long lastTimestamp, DbConnection connection, DbTransaction transaction = null)
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

            ((SqlParameterCollection)cmd.Parameters)["@changeTable"].TypeName = string.Empty;
            ((SqlParameterCollection)cmd.Parameters)["@changeTable"].Value = records;
            ((SqlParameterCollection)cmd.Parameters)["@sync_min_timestamp"].Value = lastTimestamp;
            ((SqlParameterCollection)cmd.Parameters)["@sync_scope_id"].Value = senderScopeId;

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                if (transaction != null)
                    cmd.Transaction = transaction;

                using (var dataReader = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                {
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


        //private static TypeConverter Int16Converter = TypeDescriptor.GetConverter(typeof(short));
        //private static TypeConverter Int32Converter = TypeDescriptor.GetConverter(typeof(int));
        //private static TypeConverter Int64Converter = TypeDescriptor.GetConverter(typeof(long));
        //private static TypeConverter UInt16Converter = TypeDescriptor.GetConverter(typeof(ushort));
        //private static TypeConverter UInt32Converter = TypeDescriptor.GetConverter(typeof(uint));
        //private static TypeConverter UInt64Converter = TypeDescriptor.GetConverter(typeof(ulong));
        //private static TypeConverter DateTimeConverter = TypeDescriptor.GetConverter(typeof(DateTime));
        //private static TypeConverter StringConverter = TypeDescriptor.GetConverter(typeof(string));
        //private static TypeConverter ByteConverter = TypeDescriptor.GetConverter(typeof(byte));
        //private static TypeConverter BoolConverter = TypeDescriptor.GetConverter(typeof(bool));
        //private static TypeConverter GuidConverter = TypeDescriptor.GetConverter(typeof(Guid));
        //private static TypeConverter CharConverter = TypeDescriptor.GetConverter(typeof(char));
        //private static TypeConverter DecimalConverter = TypeDescriptor.GetConverter(typeof(decimal));
        //private static TypeConverter DoubleConverter = TypeDescriptor.GetConverter(typeof(double));
        //private static TypeConverter FloatConverter = TypeDescriptor.GetConverter(typeof(float));
        //private static TypeConverter SByteConverter = TypeDescriptor.GetConverter(typeof(sbyte));
        //private static TypeConverter TimeSpanConverter = TypeDescriptor.GetConverter(typeof(TimeSpan));


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

        public override DbCommand GetCommand(DbCommandType nameType, SyncFilter filter)
        {
            var command = new SqlCommand();

            string text;
            bool isStoredProc;

            (text, isStoredProc) = this.sqlObjectNames.GetCommandName(nameType, filter);

            command.CommandType = isStoredProc ? CommandType.StoredProcedure : CommandType.Text;
            command.CommandText = text;

            return command;
        }

        /// <summary>
        /// Set a stored procedure parameters or text parameters
        /// </summary>
        public override async Task AddCommandParametersAsync(DbCommandType commandType, DbCommand command, DbConnection connection, DbTransaction transaction = null, SyncFilter filter = null)
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
                this.SetUpdateRowMetadataParameters(command);
                return;
            }

            if (commandType == DbCommandType.UpdateRow)
            {
                this.SetUpdateRowParameters(command);
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

                if (transaction != null)
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
                    // TODO: Fix that.
                    //SqlCommandBuilder.DeriveParameters((SqlCommand)command);

                    await ((SqlConnection)connection).DeriveParametersAsync((SqlCommand)command, false, (SqlTransaction)transaction).ConfigureAwait(false);

                    var arrayParameters = new List<SqlParameter>();
                    foreach (var p in command.Parameters)
                        arrayParameters.Add(((SqlParameter)p).Clone());

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
                var sqlParameter = (SqlParameter)parameter;

                // try to get the source column (from the SchemaTable)
                var sqlParameterName = sqlParameter.ParameterName.Replace("@", "");
                var colDesc = TableDescription.Columns.FirstOrDefault(c => c.ColumnName.Equals(sqlParameterName, SyncGlobalization.DataSourceStringComparison));

                if (colDesc != null && !string.IsNullOrEmpty(colDesc.ColumnName))
                    sqlParameter.SourceColumn = colDesc.ColumnName;
            }
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

        private void SetUpdateRowMetadataParameters(DbCommand command)
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
