using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Log;
using Microsoft.SqlServer.Server;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;


namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlSyncAdapter : DbSyncAdapter
    {
        private SqlConnection connection;
        private SqlTransaction transaction;
        private SqlObjectNames sqlObjectNames;

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
        }

        private SqlMetaData GetSqlMetadaFromType(DmColumn column)
        {
            var sqlDbType = column.GetSqlDbType();
            var dbType = column.DbType;
            var precision = column.GetSqlTypePrecision();
            int maxLength = column.MaxLength;

            if (sqlDbType == SqlDbType.VarChar || sqlDbType == SqlDbType.NVarChar)
            {
                maxLength = column.MaxLength <= 0 ? ((sqlDbType == SqlDbType.NVarChar) ? 4000 : 8000) : column.MaxLength;
                return new SqlMetaData(column.ColumnName, sqlDbType, maxLength);
            }

            if (column.DataType == typeof(char))
                return new SqlMetaData(column.ColumnName, sqlDbType, 1);

            if (sqlDbType == SqlDbType.Char || sqlDbType == SqlDbType.NChar)
            {
                maxLength = column.MaxLength <= 0 ? (sqlDbType == SqlDbType.NChar ? 4000 : 8000) : column.MaxLength;
                return new SqlMetaData(column.ColumnName, sqlDbType, maxLength);
            }

            if (sqlDbType == SqlDbType.Binary || sqlDbType == SqlDbType.VarBinary)
            {
                maxLength = column.MaxLength <= 0 ? 8000 : column.MaxLength;
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
        public override void ExecuteBatchCommand(DbCommand cmd, DmTable applyTable, DmTable failedRows, ScopeInfo scope)
        {
            if (applyTable.Rows.Count <= 0)
                return;

            var lstMutableColumns = applyTable.Columns.Where(c => !c.ReadOnly).ToList();

            List<SqlDataRecord> records = new List<SqlDataRecord>(applyTable.Rows.Count);
            SqlMetaData[] metadatas = new SqlMetaData[lstMutableColumns.Count];

            for (int i = 0; i < lstMutableColumns.Count; i++)
            {
                var column = lstMutableColumns[i];

                SqlMetaData metadata = GetSqlMetadaFromType(column);
                metadatas[i] = metadata;
            }

            foreach (var dmRow in applyTable.Rows)
            {
                SqlDataRecord record = new SqlDataRecord(metadatas);

                int sqlMetadataIndex = 0;
                for (int i = 0; i < dmRow.ItemArray.Length; i++)
                {
                    // check if it's readonly
                    if (applyTable.Columns[i].ReadOnly)
                        continue;

                    dynamic defaultValue = applyTable.Columns[i].DefaultValue;
                    dynamic rowValue = dmRow[i];

                    if (applyTable.Columns[i].AllowDBNull && rowValue == null)
                        rowValue = DBNull.Value;
                    else if (rowValue == null)
                        rowValue = defaultValue;

                    record.SetValue(sqlMetadataIndex, rowValue);
                    sqlMetadataIndex++;
                }
                records.Add(record);
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
                //DbException dbException = dbException1;
                //Error = CheckZombieTransaction(tvpCommandNameForApplyType, Adapter.TableName, dbException);
                //this.AddFailedRowsAfterRIFailure(applyTable, failedRows);
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
                Logger.Current.Error($"DeriveParameters failed : {ex}");
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
