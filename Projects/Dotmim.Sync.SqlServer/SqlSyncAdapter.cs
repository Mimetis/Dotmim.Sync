using Dotmim.Sync.Builders;

using Dotmim.Sync.Filter;
using Dotmim.Sync.SqlServer.Manager;
using Microsoft.SqlServer.Server;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.Linq;


namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlSyncAdapter : DbSyncAdapter
    {
        private SqlConnection connection;
        private SqlTransaction transaction;
        private SqlObjectNames sqlObjectNames;
        private SqlDbMetadata sqlMetadata;

        // Derive Parameters cache
        // Be careful, we can have collision between databases
        // this static class could be shared accross databases with same command name
        // but different table schema
        // So the string should contains the connection string as well
        private static ConcurrentDictionary<string, List<SqlParameter>> derivingParameters
            = new ConcurrentDictionary<string, List<SqlParameter>>();

        public override DbConnection Connection
        {
            get
            {
                return this.connection;
            }
        }
        public override DbTransaction Transaction
        {
            get
            {
                return this.transaction;
            }

        }

        public SqlSyncAdapter(SyncTable tableDescription, DbConnection connection, DbTransaction transaction) : base(tableDescription)
        {
            var sqlc = connection as SqlConnection;
            this.connection = sqlc ?? throw new InvalidCastException("Connection should be a SqlConnection");

            this.transaction = transaction as SqlTransaction;

            this.sqlObjectNames = new SqlObjectNames(tableDescription);
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


        /// <summary>
        /// Executing a batch command
        /// </summary>
        public override void ExecuteBatchCommand(DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> applyRows, SyncTable schemaChangesTable, SyncTable failedRows, long lastTimestamp)
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
                                case SqlDbType.DateTimeOffset:
                                    if (columnType != typeof(DateTime))
                                        rowValue = SyncTypeConverter.TryConvertTo<DateTime>(rowValue);
                                    break;
                                case SqlDbType.Decimal:
                                    if (columnType != typeof(decimal))
                                        rowValue = SyncTypeConverter.TryConvertTo<decimal>(rowValue);
                                    break;
                                case SqlDbType.Float:
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

            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    this.connection.Open();

                if (this.transaction != null)
                    cmd.Transaction = this.transaction;

                using (var dataReader = cmd.ExecuteReader())
                {
                    while (dataReader.Read())
                    {
                        var itemArray = new object[dataReader.FieldCount];
                        for (var i = 0; i < dataReader.FieldCount; i++)
                        {
                            var columnValueObject = dataReader.GetValue(i);
                            var columnValue = columnValueObject == DBNull.Value ? null : columnValueObject;
                            itemArray[i] = columnValue;
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

                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }
        }


        private static TypeConverter Int16Converter = TypeDescriptor.GetConverter(typeof(short));
        private static TypeConverter Int32Converter = TypeDescriptor.GetConverter(typeof(int));
        private static TypeConverter Int64Converter = TypeDescriptor.GetConverter(typeof(long));
        private static TypeConverter UInt16Converter = TypeDescriptor.GetConverter(typeof(ushort));
        private static TypeConverter UInt32Converter = TypeDescriptor.GetConverter(typeof(uint));
        private static TypeConverter UInt64Converter = TypeDescriptor.GetConverter(typeof(ulong));
        private static TypeConverter DateTimeConverter = TypeDescriptor.GetConverter(typeof(DateTime));
        private static TypeConverter StringConverter = TypeDescriptor.GetConverter(typeof(string));
        private static TypeConverter ByteConverter = TypeDescriptor.GetConverter(typeof(byte));
        private static TypeConverter BoolConverter = TypeDescriptor.GetConverter(typeof(bool));
        private static TypeConverter GuidConverter = TypeDescriptor.GetConverter(typeof(Guid));
        private static TypeConverter CharConverter = TypeDescriptor.GetConverter(typeof(char));
        private static TypeConverter DecimalConverter = TypeDescriptor.GetConverter(typeof(decimal));
        private static TypeConverter DoubleConverter = TypeDescriptor.GetConverter(typeof(double));
        private static TypeConverter FloatConverter = TypeDescriptor.GetConverter(typeof(float));
        private static TypeConverter SByteConverter = TypeDescriptor.GetConverter(typeof(sbyte));
        private static TypeConverter TimeSpanConverter = TypeDescriptor.GetConverter(typeof(TimeSpan));


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

        public override DbCommand GetCommand(DbCommandType nameType, IEnumerable<SyncFilter> filters = null)
        {
            var command = this.Connection.CreateCommand() as SqlCommand;

            string text;
            bool isStoredProc;
            if (filters != null && filters.Count() > 0)
                (text, isStoredProc) = this.sqlObjectNames.GetCommandName(nameType, filters);
            else
                (text, isStoredProc) = this.sqlObjectNames.GetCommandName(nameType);

            command.CommandType = isStoredProc ? CommandType.StoredProcedure : CommandType.Text;
            command.CommandText = text;
            command.Connection = Connection as SqlConnection;

            if (Transaction != null)
                command.Transaction = Transaction as SqlTransaction;

            return command;
        }

        /// <summary>
        /// Set a stored procedure parameters
        /// </summary>
        public override void SetCommandParameters(DbCommandType commandType, DbCommand command, IEnumerable<SyncFilter> filters = null)
        {
            if (command == null)
                return;

            if (command.Parameters != null && command.Parameters.Count > 0)
                return;


            // special case for constraint
            if (commandType == DbCommandType.DisableConstraints || commandType == DbCommandType.EnableConstraints)
            {
                string check = commandType == DbCommandType.DisableConstraints ? "NOCHECK" : "CHECK";

                var p = command.CreateParameter();
                p.ParameterName = "@command1";
                p.DbType = DbType.String;
                p.Value = $"ALTER TABLE ? {check} CONSTRAINT ALL";
                command.Parameters.Add(p);

                return;
            }




            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    this.connection.Open();

                if (this.transaction != null)
                    command.Transaction = this.transaction;

                var textParser = ParserName.Parse(command.CommandText).Unquoted().Normalized().ToString();

                var source = this.connection.Database;

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

                    var parameters = connection.DeriveParameters((SqlCommand)command, false, transaction);

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
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();
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

    }
}
