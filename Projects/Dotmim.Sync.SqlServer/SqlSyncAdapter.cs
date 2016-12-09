using Dotmim.Sync.Core.Adapter;
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

namespace Dotmim.Sync.SqlServer
{
    public class SqlSyncAdapter : SyncAdapter
    {

        SqlMetaData GetSqlMetadaFromType(Type dataType, string name)
        {

            SqlMetaData smd = null;

            if (dataType == typeof(bool))
                smd = new SqlMetaData(name, SqlDbType.Bit);
            else if (dataType == typeof(byte))
                smd = new SqlMetaData(name, SqlDbType.TinyInt);
            else if (dataType == typeof(char))
                smd = new SqlMetaData(name, SqlDbType.NVarChar, 1);
            else if (dataType == typeof(DateTime))
                smd = new SqlMetaData(name, SqlDbType.DateTime);
            else if (dataType == typeof(decimal))
                smd = new SqlMetaData(name, SqlDbType.Decimal);
            else if (dataType == typeof(double))
                smd = new SqlMetaData(name, SqlDbType.Float);
            else if (dataType == typeof(Int16))
                smd = new SqlMetaData(name, SqlDbType.SmallInt);
            else if (dataType == typeof(Int32))
                smd = new SqlMetaData(name, SqlDbType.Int);
            else if (dataType == typeof(long))
                smd = new SqlMetaData(name, SqlDbType.BigInt);
            else if (dataType == typeof(Single))
                smd = new SqlMetaData(name, SqlDbType.Real);
            else if (dataType == typeof(string))
                smd = new SqlMetaData(name, SqlDbType.NVarChar);
            else if (dataType == typeof(byte[]))
                smd = new SqlMetaData(name, SqlDbType.VarBinary);
            else if (dataType == typeof(char[]))
                smd = new SqlMetaData(name, SqlDbType.NVarChar);
            else if (dataType == typeof(bool?))
                smd = new SqlMetaData(name, SqlDbType.Bit);
            else if (dataType == typeof(Byte?))
                smd = new SqlMetaData(name, SqlDbType.TinyInt);
            else if (dataType == typeof(DateTime?))
                smd = new SqlMetaData(name, SqlDbType.DateTime);
            else if (dataType == typeof(decimal?))
                smd = new SqlMetaData(name, SqlDbType.Decimal);
            else if (dataType == typeof(double?))
                smd = new SqlMetaData(name, SqlDbType.Float);
            else if (dataType == typeof(short?))
                smd = new SqlMetaData(name, SqlDbType.SmallInt);
            else if (dataType == typeof(int?))
                smd = new SqlMetaData(name, SqlDbType.Int);
            else if (dataType == typeof(long?))
                smd = new SqlMetaData(name, SqlDbType.BigInt);
            else if (dataType == typeof(float?))
                smd = new SqlMetaData(name, SqlDbType.Real);
            else if (dataType == typeof(Guid))
                smd = new SqlMetaData(name, SqlDbType.UniqueIdentifier);
            else if (dataType == typeof(Guid?))
                smd = new SqlMetaData(name, SqlDbType.UniqueIdentifier);
            else if (dataType == typeof(object))
                smd = new SqlMetaData(name, SqlDbType.Variant);
            else if (dataType == typeof(TimeSpan))
                smd = new SqlMetaData(name, SqlDbType.Time);
            else if (dataType == typeof(TimeSpan?))
                smd = new SqlMetaData(name, SqlDbType.Time);
            else if (dataType == typeof(DateTimeOffset))
                smd = new SqlMetaData(name, SqlDbType.DateTimeOffset);
            else if (dataType == typeof(DateTimeOffset?))
                smd = new SqlMetaData(name, SqlDbType.DateTimeOffset);
            else
                throw new Exception($"Unknown Data Type Code({dataType}, {dataType.GetTypeInfo().FullName}");

            return smd;
        }

        public override void ExecuteBatchCommand(DbCommand cmd, DmTable applyTable, DmTable failedRows, ScopeInfo scope)
        {
            if (applyTable.Rows.Count <= 0)
                return;

            List<SqlDataRecord> records = new List<SqlDataRecord>(applyTable.Rows.Count);
            SqlMetaData[] metadatas = new SqlMetaData[applyTable.Columns.Count];
            for (int i = 0; i < applyTable.Columns.Count; i++)
            {
                var column = applyTable.Columns[i];

                SqlMetaData metadata = GetSqlMetadaFromType(column.DataType, column.ColumnName);
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
            ((SqlParameterCollection)cmd.Parameters)["@sync_scope_name"].Value = scope.Name;
            ((SqlParameterCollection)cmd.Parameters)["@sync_min_timestamp"].Value = scope.LastTimestamp;

            try
            {
                using (DbDataReader dataReader = cmd.ExecuteReader())
                {
                    failedRows.Fill(dataReader);
                }
            }
            catch (DbException dbException1)
            {
                //DbException dbException = dbException1;
                //Error = CheckZombieTransaction(tvpCommandNameForApplyType, Adapter.TableName, dbException);
                //this.AddFailedRowsAfterRIFailure(applyTable, failedRows);
            }
        }

        public override bool IsPrimaryKeyViolation(Exception Error)
        {
            SqlException error = Error as SqlException;
            if (error != null && error.Number == 2627)
                return true;

            return false;
        }

        public override void SetCommandSessionParameters(DbCommand command, ScopeConfigDataAdapter config)
        {
            if (command == null)
                return;

            if (command.Parameters != null && command.Parameters.Count > 0)
                return;

            var alreadyOpened = command.Connection.State != ConnectionState.Closed;

            try
            {
                // Get parameters
                if (!alreadyOpened)
                    command.Connection.Open();

                SqlConnection sqlConnection = command.Connection as SqlConnection;

                if (sqlConnection == null)
                    throw new InvalidCastException("the connection must be a SqlConnection to be able to derive parameters");

                sqlConnection.DeriveParameters((SqlCommand)command);

                if (command.Parameters[0].ParameterName == "@RETURN_VALUE")
                    command.Parameters.RemoveAt(0);

                if (!alreadyOpened)
                    command.Connection.Close();

            }
            catch (Exception ex)
            {
                Logger.Current.Error($"DeriveParameters failed : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && command.Connection.State != ConnectionState.Closed)
                    command.Connection.Close();
            }

            foreach (var parameter in command.Parameters)
            {
                var sqlParameter = (SqlParameter)parameter;

                var colDesc = config.Columns.FirstOrDefault(c => string.Equals(c.ParameterName, sqlParameter.ParameterName, StringComparison.CurrentCultureIgnoreCase));

                if (colDesc != null && !string.IsNullOrEmpty(colDesc.UnquotedName))
                    sqlParameter.SourceColumn = colDesc.UnquotedName;

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
