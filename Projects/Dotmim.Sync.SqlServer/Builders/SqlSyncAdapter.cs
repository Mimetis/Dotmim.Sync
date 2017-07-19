using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dotmim.Sync.Core.Scope;
using Dotmim.Sync.Data;
using System.Data.Common;
using System.Data.SqlClient;
using Dotmim.Sync.Core.Log;
using Microsoft.SqlServer.Server;
using System.Data;
using System.Data.SqlTypes;
using System.Reflection;
using Dotmim.Sync.Core.Builders;
using Dotmim.Sync.Core.Common;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlSyncAdapter : DbSyncAdapter
    {
        private SqlConnection connection;
        private SqlTransaction transaction;

        public override DmTable TableDescription { get; set; }
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

        public SqlSyncAdapter(DbConnection connection, DbTransaction transaction)
        {
            var sqlc = connection as SqlConnection;
            this.connection = sqlc ?? throw new InvalidCastException("Connection should be a SqlConnection");

            this.transaction = transaction as SqlTransaction;
        }

        private SqlMetaData GetSqlMetadaFromType(DmColumn column)
        {
            SqlMetaData smd = null;

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

            smd = new SqlMetaData(column.ColumnName, sqlDbType);

            return smd;
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

            List<SqlDataRecord> records = new List<SqlDataRecord>(applyTable.Rows.Count);
            SqlMetaData[] metadatas = new SqlMetaData[applyTable.Columns.Count];
            for (int i = 0; i < applyTable.Columns.Count; i++)
            {
                var column = applyTable.Columns[i];

                SqlMetaData metadata = GetSqlMetadaFromType(column);
                metadatas[i] = metadata;
            }

            foreach (var dmRow in applyTable.Rows)
            {
                SqlDataRecord record = new SqlDataRecord(metadatas);
                for (int i = 0; i < dmRow.ItemArray.Length; i++)
                {
                    var c = dmRow.Table.Columns[i];
                    dynamic defaultValue = c.DefaultValue;
                    dynamic rowValue = dmRow[i];

                    if (c.AllowDBNull && rowValue == null)
                        rowValue = DBNull.Value;
                    else if (rowValue == null)
                        rowValue = defaultValue;

                    record.SetValue(i, rowValue);
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


        // Derive Parameters cache
        private static Dictionary<string, List<SqlParameter>> derivingParameters = new Dictionary<string, List<SqlParameter>>();

        /// <summary>
        /// Set a stored procedure parameters
        /// </summary>
        public override void SetCommandSessionParameters(DbCommand command)
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
                    foreach(var p in derivingParameters[textParser.UnquotedString])
                        command.Parameters.Add(p.Clone());
                }
                else
                {
                    var parameters = connection.DeriveParameters((SqlCommand)command, false, transaction);

                    var arrayParameters = new List<SqlParameter>();
                    foreach(var p in parameters)
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

                #region ....
                //    if (colDesc != null)
                //    {
                //        sqlParameter.ParameterName = colDesc.ParameterName;
                //        sqlParameter.DbType = DbHelper.GetDbTypeFromString(colDesc.Type);
                //        sqlParameter.SourceColumn = colDesc.UnquotedName;
                //        sqlParameter.IsNullable = colDesc.IsNullable;

                //        if (colDesc.Type == null)
                //            continue;

                //        switch (colDesc.Type)
                //        {
                //            case "decimal":
                //            case "numeric":
                //                {
                //                    if (colDesc.PrecisionSpecified)
                //                        sqlParameter.Precision = (byte)colDesc.Precision;

                //                    if (!colDesc.ScaleSpecified)
                //                        break;

                //                    sqlParameter.Scale = (byte)colDesc.Scale;
                //                    break;
                //                }
                //            case "binary":
                //            case "varbinary":
                //            case "varchar":
                //            case "char":
                //            case "nvarchar":
                //            case "nchar":
                //                {
                //                    if (!colDesc.SizeSpecified)
                //                        break;

                //                    if (!string.Equals(colDesc.Size, "max", StringComparison.OrdinalIgnoreCase))
                //                    {
                //                        sqlParameter.Size = int.Parse(colDesc.Size, CultureInfo.InvariantCulture);
                //                        break;
                //                    }

                //                    sqlParameter.Size = -1;
                //                    break;
                //                }

                //        }
                //    }
                #endregion

            }
        }


    }
}
