using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.OracleClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Oracle.Builder;
using Dotmim.Sync.Oracle.Manager;

namespace Dotmim.Sync.Oracle
{
    public class OracleSyncAdapter : DbSyncAdapter
    {
        private readonly OracleConnection _connection;
        private readonly OracleTransaction _transaction;
        private OracleObjectNames oracleObjectNames;
        private OracleDbMetadata oracleMetadata;

        // Derive Parameters cache
        private static Dictionary<string, List<OracleParameter>> derivingParameters = new Dictionary<string, List<OracleParameter>>();

        public OracleSyncAdapter(DmTable tableDescription) : base(tableDescription)
        {
        }

        public OracleSyncAdapter(DmTable tableDescription, DbConnection connection, DbTransaction transaction)
            : this(tableDescription)
        {
            var sqlc = connection as OracleConnection;
            this._connection = sqlc ?? throw new InvalidCastException("Connection should be a OracleConnection");

            _transaction = transaction as OracleTransaction;
            this.oracleObjectNames = new OracleObjectNames(tableDescription);
            this.oracleMetadata = new OracleDbMetadata();
        }

        public override DbConnection Connection => _connection;

        public override DbTransaction Transaction => _transaction;

        public override void ExecuteBatchCommand(DbCommand cmd, DmView applyTable, DmTable failedRows, ScopeInfo scope)
        {
            if (applyTable.Count <= 0)
                return;

            // Insert data in bulk table 
            BulkInsertTemporyTable(BulkInsertTemporyTableText(applyTable));

            ((OracleParameterCollection)cmd.Parameters)["sync_scope_id"].Value = scope.Id;
            ((OracleParameterCollection)cmd.Parameters)["sync_min_timestamp"].Value = scope.LastTimestamp;

            bool alreadyOpened = this._connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    this._connection.Open();

                if (this._transaction != null)
                    cmd.Transaction = this._transaction;

                using (DbDataReader dataReader = cmd.ExecuteReader())
                {
                    failedRows.Fill(dataReader);
                }
            }
            catch (DbException ex)
            {
                Debug.WriteLine(ex.Message);
                throw;
            }
            finally
            {
                if (!alreadyOpened && this._connection.State != ConnectionState.Closed)
                    this._connection.Close();
            }
        }

        public override DbCommand GetCommand(DbCommandType commandType, IEnumerable<string> additionals = null)
        {
            var command = this.Connection.CreateCommand();

            string text;
            if (additionals != null)
                text = this.oracleObjectNames.GetCommandName(commandType, additionals);
            else
                text = this.oracleObjectNames.GetCommandName(commandType);

            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = text;
            command.Connection = Connection;

            if (Transaction != null)
                command.Transaction = Transaction;

            return command;
        }

        public override bool IsPrimaryKeyViolation(Exception Error)
        {
            OracleException error = Error as OracleException;
            if (error != null && error.Code == 2627)
                return true;
            return false;
        }

        public override void SetCommandParameters(DbCommandType commandType, DbCommand command)
        {
            if (command == null)
                return;

            if (command.Parameters != null && command.Parameters.Count > 0)
                return;

            bool alreadyOpened = this._connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    this._connection.Open();

                if (this._transaction != null)
                    command.Transaction = this._transaction;

                var textParser = new ObjectNameParser(command.CommandText);

                if (derivingParameters.ContainsKey(textParser.UnquotedString))
                {
                    foreach (var p in derivingParameters[textParser.UnquotedString])
                        command.Parameters.Add(p.Clone());
                }
                else
                {
                    var parameters = _connection.DeriveParameters((OracleCommand)command, true, _transaction);

                    var arrayParameters = new List<OracleParameter>();
                    foreach (var p in parameters)
                        arrayParameters.Add(p.Clone());

                    derivingParameters.Add(textParser.UnquotedString, arrayParameters);
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"DeriveParameters failed : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && this._connection.State != ConnectionState.Closed)
                    this._connection.Close();
            }


            foreach (var parameter in command.Parameters)
            {
                var sqlParameter = (OracleParameter)parameter;

                // try to get the source column (from the dmTable)
                var sqlParameterName = sqlParameter.ParameterName.Replace("@", "");
                sqlParameterName = sqlParameterName.Remove(sqlParameterName.Length - 1, 1);
                var colDesc = TableDescription.Columns.FirstOrDefault(c => string.Equals(c.ColumnName, sqlParameterName, StringComparison.CurrentCultureIgnoreCase));

                if (colDesc != null && !string.IsNullOrEmpty(colDesc.ColumnName))
                    sqlParameter.SourceColumn = colDesc.ColumnName;
            }
        }

        #region Virtual

        protected override void PostExecuteBatchCommand()
        {
            var command = _connection.CreateCommand();
            if (_transaction != null)
                command.Transaction = _transaction;
            bool alreadyOpened = _connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    _connection.Open();

                command.CommandType = CommandType.Text;
                command.CommandText = this.TruncateTableTemporyBulkText();
                command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during PostExecuteBatchCommand : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && _connection.State != ConnectionState.Closed)
                    _connection.Close();

                if (command != null)
                    command.Dispose();
            }            
        }

        protected override void PreExecuteBatchCommand()
        {
            var command = _connection.CreateCommand();
            if (_transaction != null)
                command.Transaction = _transaction;
            bool alreadyOpened = _connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    _connection.Open();

                command.CommandType = CommandType.Text;
                command.CommandText = this.TruncateTableTemporyBulkText();
                command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during PreExecuteBatchCommand : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && _connection.State != ConnectionState.Closed)
                    _connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }

        #endregion

        #region Private Builder for BULK Operation

        private string TruncateTableTemporyBulkText()
        {
            var bulkTemporyTableName = oracleObjectNames.GetCommandName(DbCommandType.BulkTemporyTable);
            var bulkTableName = oracleObjectNames.GetCommandName(DbCommandType.BulkTable);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"BEGIN");
            stringBuilder.AppendLine($"execute immediate 'TRUNCATE TABLE {bulkTableName}';");
            stringBuilder.AppendLine($"execute immediate 'TRUNCATE TABLE {bulkTemporyTableName}';");
            stringBuilder.AppendLine($"END;");
            return stringBuilder.ToString();
        }

        private string BulkInsertTemporyTableText(DmView applyTable)
        {
            var bulkTableName = oracleObjectNames.GetCommandName(DbCommandType.BulkTable);

            StringBuilder sb = new StringBuilder();
            StringBuilder sbInsert = new StringBuilder();
            string str = "", exceptionMessage = "Can't convert value {0} to {1}";
            var lstMutableColumns = applyTable.Table.Columns.Where(c => !c.ReadOnly).ToList();

            sb.AppendLine("BEGIN");
            sbInsert.Append($"INSERT INTO {bulkTableName}(");

            lstMutableColumns.ToList().ForEach(c => {
                sbInsert.Append($"{str} {c.ColumnName}");
                str = ",";
            });
            sbInsert.Append(") VALUES (");
            foreach (var dmRow in applyTable)
            {
                str = "";
                sb.Append(sbInsert.ToString());

                bool isDeleted = false;
                // Cancel the delete state to be able to get the row, more simplier
                if (dmRow.RowState == DmRowState.Deleted)
                {
                    isDeleted = true;
                    dmRow.RejectChanges();
                }

                for (int i = 0; i < dmRow.ItemArray.Length; i++)
                {
                    dynamic rowValue = dmRow[i];
                    var columnType = applyTable.Table.Columns[i].DataType;

                    OracleType type = oracleMetadata.ValidateOracleType(columnType);

                    if (dmRow[i] != null)
                    {
                        switch (type)
                        {
                            case OracleType.Number:
                                if (columnType == typeof(Int32))
                                {
                                    if (Int32.TryParse(rowValue.ToString(), out Int32 v0))
                                        rowValue = v0;
                                    else
                                        throw new InvalidCastException(string.Format(exceptionMessage, rowValue, columnType));
                                }
                                else if (columnType == typeof(Int16))
                                {
                                    if (Int16.TryParse(rowValue.ToString(), out Int16 v1))
                                        rowValue = v1;
                                    else
                                        throw new InvalidCastException(string.Format(exceptionMessage, rowValue, columnType));
                                }
                                else if (columnType == typeof(Int64))
                                {
                                    if (Int64.TryParse(rowValue.ToString(), out Int64 v2))
                                        rowValue = v2;
                                    else
                                        throw new InvalidCastException(string.Format(exceptionMessage, rowValue, columnType));
                                }
                                break;
                            case OracleType.Blob:
                                if(columnType == typeof(byte[]))
                                {
                                    if (rowValue is byte[])
                                        rowValue = (byte[])rowValue;
                                    else
                                        throw new InvalidCastException(string.Format(exceptionMessage, rowValue, columnType));
                                }
                                break;
                            case OracleType.NVarChar:
                                    rowValue = $"'{Convert.ToString(rowValue)}'";
                                break;
                            case OracleType.Byte:
                                if(columnType == typeof(Boolean))
                                {
                                    if (Int32.TryParse(rowValue.ToString(), out Int32 v4))
                                        rowValue = v4 == 1;
                                    else
                                        throw new InvalidCastException(string.Format(exceptionMessage, rowValue, columnType));
                                }
                                break;
                            case OracleType.DateTime:
                                if(columnType == typeof(DateTime))
                                {
                                    if (DateTime.TryParse(rowValue.ToString(), out DateTime v5))
                                        rowValue = v5;
                                    else
                                        throw new InvalidCastException(string.Format(exceptionMessage, rowValue, columnType));

                                }
                                break;
                            case OracleType.Float:
                                if(columnType == typeof(float))
                                {
                                    if (float.TryParse(rowValue.ToString(), out float v6))
                                        rowValue = v6;
                                    else
                                        throw new InvalidCastException(string.Format(exceptionMessage, rowValue, columnType));

                                }
                                break;
                            case OracleType.Double:
                                if (double.TryParse(rowValue.ToString(), out double v))
                                {
                                    rowValue = v;
                                }
                                else
                                    throw new InvalidCastException(string.Format(exceptionMessage, rowValue, columnType));
                                break;

                        }
                        sb.Append($"{str} {rowValue.ToString()}");
                    }
                    else
                        sb.Append($"{str} null");

                    str = ",";
                }
                sb.AppendLine(");");

                if (isDeleted)
                    dmRow.Delete();
            }
            sb.AppendLine("END;");
            return sb.ToString();
        }

        private void BulkInsertTemporyTable(String commandText)
        {
            bool alreadyOpened = this._connection.State == ConnectionState.Open;

            var command = this.Connection.CreateCommand();

            if (!alreadyOpened)
                this._connection.Open();

            command.CommandType = CommandType.Text;
            command.CommandText = commandText;
            command.Connection = Connection;

            if (Transaction != null)
                command.Transaction = Transaction;

            try
            {
                command.ExecuteNonQuery();
            }
            finally
            {
                command?.Dispose();

                if (!alreadyOpened && this._connection.State != ConnectionState.Closed)
                    this._connection.Close();                
            }
            
        }

        #endregion
    }
}
