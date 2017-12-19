using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Log;
using Dotmim.Sync.SqlServer.Manager;
using Microsoft.SqlServer.Server;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
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
        private static Dictionary<string, List<SqlParameter>> derivingParameters = new Dictionary<string, List<SqlParameter>>();

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

        public SqlSyncAdapter(DmTable tableDescription, DbConnection connection, DbTransaction transaction) : base(tableDescription)
        {
            var sqlc = connection as SqlConnection;
            this.connection = sqlc ?? throw new InvalidCastException("Connection should be a SqlConnection");

            this.transaction = transaction as SqlTransaction;

            this.sqlObjectNames = new SqlObjectNames(tableDescription);
            this.sqlMetadata = new SqlDbMetadata();
        }

        private SqlMetaData GetSqlMetadaFromType(DmColumn column)
        {
            var dbType = column.DbType;
            var precision = column.Precision;
            long maxLength = (long)column.MaxLength;

            SqlDbType sqlDbType = (SqlDbType)this.sqlMetadata.TryGetOwnerDbType(column.OriginalDbType, column.DbType, false, false, this.TableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

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

            if (column.DataType == typeof(char))
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


            return new SqlMetaData(column.ColumnName, sqlDbType);

        }


        /// <summary>
        /// Executing a batch command
        /// </summary>
        /// <param name="cmd">the DbCommand already prepared</param>
        /// <param name="applyTable">the table rows to apply</param>
        /// <param name="failedRows">the failed rows dmTable to store failed rows</param>
        /// <param name="scope">the current scope</param>
        public override void ExecuteBatchCommand(DbCommand cmd, DmView applyTable, DmTable failedRows, ScopeInfo scope)
        {
            if (applyTable.Count <= 0)
                return;

            var lstMutableColumns = applyTable.Table.Columns.Where(c => !c.ReadOnly).ToList();

            List<SqlDataRecord> records = new List<SqlDataRecord>(applyTable.Count);
            SqlMetaData[] metadatas = new SqlMetaData[lstMutableColumns.Count];

            for (int i = 0; i < lstMutableColumns.Count; i++)
            {
                var column = lstMutableColumns[i];

                SqlMetaData metadata = GetSqlMetadaFromType(column);
                metadatas[i] = metadata;
            }
            try
            {
                foreach (var dmRow in applyTable)
                {
                    SqlDataRecord record = new SqlDataRecord(metadatas);

                    int sqlMetadataIndex = 0;
                    bool isDeleted = false;

                    // Cancel the delete state to be able to get the row, more simplier
                    if (dmRow.RowState == DmRowState.Deleted)
                    {
                        isDeleted = true;
                        dmRow.RejectChanges();
                    }

                    for (int i = 0; i < dmRow.ItemArray.Length; i++)
                    {
                        // check if it's readonly
                        if (applyTable.Table.Columns[i].ReadOnly)
                            continue;

                        // Get the default value
                        // Since we have the readonly values in ItemArray, get the value from original column
                        dynamic defaultValue = applyTable.Table.Columns[i].DefaultValue;
                        dynamic rowValue = dmRow[i];
                        var columnType = applyTable.Table.Columns[i].DataType;

                        // metadatas don't have readonly values, so get from sqlMetadataIndex
                        var sqlMetadataType = metadatas[sqlMetadataIndex].SqlDbType;
                        if (rowValue != null)
                        {
                            switch (sqlMetadataType)
                            {
                                case SqlDbType.BigInt:
                                    if (columnType != typeof(long))
                                        if (Int64.TryParse(rowValue.ToString(), out Int64 v))
                                            rowValue = v;
                                        else
                                            throw new SyncException($"Can't convert value {rowValue} to Int64");
                                    break;
                                case SqlDbType.Bit:
                                    if (columnType != typeof(bool))
                                        if (Boolean.TryParse(rowValue.ToString(), out Boolean v))
                                            rowValue = v;
                                        else
                                            throw new SyncException($"Can't convert value {rowValue} to Boolean");
                                    break;
                                case SqlDbType.Date:
                                case SqlDbType.DateTime:
                                case SqlDbType.DateTime2:
                                case SqlDbType.SmallDateTime:
                                    if (columnType != typeof(DateTime))
                                        if (DateTime.TryParse(rowValue.ToString(), out DateTime v))
                                            rowValue = v;
                                        else
                                            throw new SyncException($"Can't convert value {rowValue} to DateTime");
                                    break;
                                case SqlDbType.DateTimeOffset:
                                    if (columnType != typeof(DateTimeOffset))
                                    {
                                        if (DateTimeOffset.TryParse(rowValue.ToString(), out DateTimeOffset dt))
                                            rowValue = dt;
                                        else
                                            throw new SyncException($"Can't convert value {rowValue} to DateTimeOffset");
                                    }
                                    break;
                                case SqlDbType.Decimal:
                                    if (columnType != typeof(Decimal))
                                        if (Decimal.TryParse(rowValue.ToString(), out decimal v))
                                            rowValue = v;
                                        else
                                            throw new SyncException($"Can't convert value {rowValue} to Decimal");
                                    break;
                                case SqlDbType.Float:
                                    if (columnType != typeof(Double))
                                        if (Double.TryParse(rowValue.ToString(), out Double v))
                                            rowValue = v;
                                        else
                                            throw new SyncException($"Can't convert value {rowValue} to Double");
                                    break;
                                case SqlDbType.Real:
                                    if (columnType != typeof(float))
                                        if (float.TryParse(rowValue.ToString(), out float v))
                                            rowValue = v;
                                        else
                                            throw new SyncException($"Can't convert value {rowValue} to Double");
                                    break;
                                case SqlDbType.Image:
                                case SqlDbType.Binary:
                                case SqlDbType.VarBinary:
                                    if (columnType != typeof(Byte[]))
                                        rowValue = BitConverter.GetBytes(rowValue);
                                    break;
                                case SqlDbType.Variant:
                                    break;
                                case SqlDbType.Int:
                                    if (columnType != typeof(Int32))
                                        if (Int32.TryParse(rowValue.ToString(), out int v))
                                            rowValue = v;
                                        else
                                            throw new SyncException($"Can't convert value {rowValue} to Int32");
                                    break;
                                case SqlDbType.Money:
                                case SqlDbType.SmallMoney:
                                    if (columnType != typeof(Decimal))
                                        if (Decimal.TryParse(rowValue.ToString(), out Decimal v))
                                            rowValue = v;
                                        else
                                            throw new SyncException($"Can't convert value {rowValue} to Decimal");
                                    break;
                                case SqlDbType.NChar:
                                case SqlDbType.NText:
                                case SqlDbType.VarChar:
                                case SqlDbType.Xml:
                                case SqlDbType.NVarChar:
                                case SqlDbType.Text:
                                case SqlDbType.Char:
                                    if (columnType != typeof(string))
                                        rowValue = rowValue.ToString();
                                    break;

                                case SqlDbType.SmallInt:
                                    if (columnType != typeof(Int16))
                                        if (Int16.TryParse(rowValue.ToString(), out Int16 v))
                                            rowValue = v;
                                        else
                                            throw new SyncException($"Can't convert value {rowValue} to Int16");
                                    break;
                                case SqlDbType.Time:
                                    if (columnType != typeof(TimeSpan))
                                        if (TimeSpan.TryParse(rowValue.ToString(), out TimeSpan v))
                                            rowValue = v;
                                        else
                                            throw new SyncException($"Can't convert value {rowValue} to TimeSpan");
                                    break;
                                case SqlDbType.Timestamp:
                                    break;
                                case SqlDbType.TinyInt:
                                    if (columnType != typeof(Byte))
                                        if (Byte.TryParse(rowValue.ToString(), out byte v))
                                            rowValue = v;
                                        else
                                            throw new SyncException($"Can't convert value {rowValue} to Byte");
                                    break;
                                case SqlDbType.Udt:
                                    throw new SyncException($"Can't use UDT as SQL Type");
                                case SqlDbType.UniqueIdentifier:
                                    if (columnType != typeof(Guid))
                                        if (Guid.TryParse(rowValue.ToString(), out Guid v))
                                            rowValue = v;
                                        else
                                            throw new SyncException($"Can't convert value {rowValue} to Guid");
                                    break;
                            }
                        }

                        if (applyTable.Table.Columns[i].AllowDBNull && rowValue == null)
                            rowValue = DBNull.Value;
                        else if (rowValue == null)
                            rowValue = defaultValue;

                        record.SetValue(sqlMetadataIndex, rowValue);
                        sqlMetadataIndex++;
                    }
                    records.Add(record);

                    // Apply the delete
                    // is it mandatory ?
                    if (isDeleted)
                        dmRow.Delete();

                }
            }
            catch (Exception ex)
            {
                throw new SyncException($"Can't create a SqlRecord based on the rows we have: {ex.Message}");
            }


            ((SqlParameterCollection)cmd.Parameters)["@changeTable"].TypeName = string.Empty;
            ((SqlParameterCollection)cmd.Parameters)["@changeTable"].Value = records;
            ((SqlParameterCollection)cmd.Parameters)["@sync_scope_id"].Value = scope.Id;
            ((SqlParameterCollection)cmd.Parameters)["@sync_min_timestamp"].Value = scope.LastTimestamp;

            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    this.connection.Open();

                if (this.transaction != null)
                    cmd.Transaction = this.transaction;


                using (DbDataReader dataReader = cmd.ExecuteReader())
                {
                    failedRows.Fill(dataReader);
                }
            }
            catch (DbException ex)
            {
                Debug.WriteLine(ex.Message);
                //DbException dbException = dbException1;
                //Error = CheckZombieTransaction(tvpCommandNameForApplyType, Adapter.TableName, dbException);
                //this.AddFailedRowsAfterRIFailure(applyTable, failedRows);
                throw;
            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }
        }

        /// <summary>
        /// Check if an exception is a primary key exception
        /// </summary>
        public override bool IsPrimaryKeyViolation(Exception Error)
        {
            SqlException error = Error as SqlException;
            if (error != null && error.Number == 2627)
                return true;

            return false;
        }

        public override DbCommand GetCommand(DbCommandType nameType, IEnumerable<string> additionals = null)
        {
            var command = this.Connection.CreateCommand();

            string text;
            if (additionals != null)
                text = this.sqlObjectNames.GetCommandName(nameType, additionals);
            else
                text = this.sqlObjectNames.GetCommandName(nameType);

            // on Sql Server, everything is Stored Procedure
            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = text;
            command.Connection = Connection;

            if (Transaction != null)
                command.Transaction = Transaction;

            return command;
        }

        /// <summary>
        /// Set a stored procedure parameters
        /// </summary>
        public override void SetCommandParameters(DbCommandType commandType, DbCommand command)
        {
            if (command == null)
                return;

            if (command.Parameters != null && command.Parameters.Count > 0)
                return;

            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    this.connection.Open();

                if (this.transaction != null)
                    command.Transaction = this.transaction;

                var textParser = new ObjectNameParser(command.CommandText);

                if (derivingParameters.ContainsKey(textParser.UnquotedString))
                {
                    foreach (var p in derivingParameters[textParser.UnquotedString])
                        command.Parameters.Add(p.Clone());
                }
                else
                {
                    var parameters = connection.DeriveParameters((SqlCommand)command, false, transaction);

                    var arrayParameters = new List<SqlParameter>();
                    foreach (var p in parameters)
                        arrayParameters.Add(p.Clone());

                    derivingParameters.Add(textParser.UnquotedString, arrayParameters);
                }

                if (command.Parameters[0].ParameterName == "@RETURN_VALUE")
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

                // try to get the source column (from the dmTable)
                var sqlParameterName = sqlParameter.ParameterName.Replace("@", "");
                var colDesc = TableDescription.Columns.FirstOrDefault(c => string.Equals(c.ColumnName, sqlParameterName, StringComparison.CurrentCultureIgnoreCase));

                if (colDesc != null && !string.IsNullOrEmpty(colDesc.ColumnName))
                    sqlParameter.SourceColumn = colDesc.ColumnName;
            }
        }


    }
}
