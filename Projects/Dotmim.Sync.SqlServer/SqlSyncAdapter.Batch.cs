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
using Dotmim.Sync.Enumerations;

namespace Dotmim.Sync.SqlServer.Builders
{
    public partial class SqlSyncAdapter : DbSyncAdapter
    {
        
        /// <summary>
        /// Executing a batch command
        /// </summary>
        public override async Task ExecuteBatchCommandAsync(DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> applyRows, SyncTable schemaChangesTable,
                                                            SyncTable failedRows, long? lastTimestamp, DbConnection connection, DbTransaction transaction = null)
        {

            var applyRowsCount = applyRows.Count();

            if (applyRowsCount <= 0)
                return;

            var syncRowState = SyncRowState.None;

            var records = new List<SqlDataRecord>(applyRowsCount);
            SqlMetaData[] metadatas = new SqlMetaData[schemaChangesTable.Columns.Count];

            for (int i = 0; i < schemaChangesTable.Columns.Count; i++)
                metadatas[i] = GetSqlMetadaFromType(schemaChangesTable.Columns[i]);

            try
            {
                foreach (var row in applyRows)
                {
                    syncRowState = row.RowState;

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
                                    if (sqlMetadataType == SqlDbType.DateTime && rowValue < SqlDateMin)
                                        rowValue = SqlDateMin;
                                    if (sqlMetadataType == SqlDbType.SmallDateTime && rowValue < SqlSmallDateMin)
                                        rowValue = SqlSmallDateMin;
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

            var sqlParameters = cmd.Parameters as SqlParameterCollection;

            sqlParameters["@changeTable"].TypeName = string.Empty;
            sqlParameters["@changeTable"].Value = records;

            if (sqlParameters.Contains("@sync_min_timestamp"))
                sqlParameters["@sync_min_timestamp"].Value = lastTimestamp.HasValue ? (object)lastTimestamp.Value : DBNull.Value;

            if (sqlParameters.Contains("@sync_scope_id"))
                sqlParameters["@sync_scope_id"].Value = senderScopeId;

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                cmd.Transaction = transaction;

                using var dataReader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);

                while (dataReader.Read())
                {
                    //var itemArray = new object[dataReader.FieldCount];
                    //var itemArray = new object[failedRows.Columns.Count];
                    var itemArray = new SyncRow(schemaChangesTable, syncRowState);
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
                    failedRows.Rows.Add(itemArray);
                }

                dataReader.Close();

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

      }
}
