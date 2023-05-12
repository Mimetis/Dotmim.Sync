using Dotmim.Sync.Enumerations;
using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlClient.Server;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

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
                            switch (sqlMetadataType)
                            {
                                case SqlDbType.BigInt:
                                    rowValue = SyncTypeConverter.TryConvertTo<long>(rowValue);
                                    break;
                                case SqlDbType.Bit:
                                    rowValue = SyncTypeConverter.TryConvertTo<bool>(rowValue);
                                    break;
                                case SqlDbType.Date:
                                case SqlDbType.DateTime:
                                case SqlDbType.DateTime2:
                                case SqlDbType.SmallDateTime:
                                    if (sqlMetadataType == SqlDbType.DateTime && rowValue < SqlDateMin)
                                        rowValue = SqlDateMin;
                                    else if (sqlMetadataType == SqlDbType.SmallDateTime && rowValue < SqlSmallDateMin)
                                        rowValue = SqlSmallDateMin;
                                    else
                                        rowValue = SyncTypeConverter.TryConvertTo<DateTime>(rowValue);
                                    break;
                                case SqlDbType.DateTimeOffset:
                                    rowValue = SyncTypeConverter.TryConvertTo<DateTimeOffset>(rowValue);
                                    break;
                                case SqlDbType.Decimal:
                                    rowValue = SyncTypeConverter.TryConvertTo<decimal>(rowValue);
                                    break;
                                case SqlDbType.Float:
                                    rowValue = SyncTypeConverter.TryConvertTo<double>(rowValue);
                                    break;
                                case SqlDbType.Real:
                                    rowValue = SyncTypeConverter.TryConvertTo<float>(rowValue);
                                    break;
                                case SqlDbType.Image:
                                case SqlDbType.Binary:
                                case SqlDbType.VarBinary:
                                    rowValue = SyncTypeConverter.TryConvertTo<byte[]>(rowValue);
                                    break;
                                case SqlDbType.Variant:
                                    break;
                                case SqlDbType.Int:
                                    rowValue = SyncTypeConverter.TryConvertTo<int>(rowValue);
                                    break;
                                case SqlDbType.Money:
                                case SqlDbType.SmallMoney:
                                    rowValue = SyncTypeConverter.TryConvertTo<decimal>(rowValue);
                                    break;
                                case SqlDbType.NChar:
                                case SqlDbType.NText:
                                case SqlDbType.VarChar:
                                case SqlDbType.Xml:
                                case SqlDbType.NVarChar:
                                case SqlDbType.Text:
                                case SqlDbType.Char:
                                    rowValue = SyncTypeConverter.TryConvertTo<string>(rowValue);
                                    break;
                                case SqlDbType.SmallInt:
                                    rowValue = SyncTypeConverter.TryConvertTo<short>(rowValue);
                                    break;
                                case SqlDbType.Time:
                                    rowValue = SyncTypeConverter.TryConvertTo<TimeSpan>(rowValue);
                                    break;
                                case SqlDbType.Timestamp:
                                    break;
                                case SqlDbType.TinyInt:
                                    rowValue = SyncTypeConverter.TryConvertTo<byte>(rowValue);
                                    break;
                                case SqlDbType.Udt:
                                    throw new ArgumentException($"Can't use UDT as SQL Type");
                                case SqlDbType.UniqueIdentifier:
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
                throw;
            }
            finally
            {
                records.Clear();

                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }
        }



        private SqlMetaData GetSqlMetadaFromType(SyncColumn column)
        {
            long maxLength = column.MaxLength;
            var dataType = column.GetDataType();

            var sqlDbType = this.TableDescription.OriginalProvider == SqlSyncProvider.ProviderType ?
                this.SqlMetadata.GetSqlDbType(column) : this.SqlMetadata.GetOwnerDbTypeFromDbType(column);

            // Since we validate length before, it's not mandatory here.
            // let's say.. just in case..
            if (sqlDbType == SqlDbType.VarChar || sqlDbType == SqlDbType.NVarChar)
            {
                // set value for (MAX) 
                maxLength = maxLength <= 0 ? SqlMetaData.Max : maxLength;

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
                var (p, s) = this.SqlMetadata.GetPrecisionAndScale(column);
                if (p > 0 && p > s)
                {
                    return new SqlMetaData(column.ColumnName, sqlDbType, p, s);
                }
                else
                {
                    if (p == 0)
                        p = 18;
                    if (s == 0)
                        s = Math.Min((byte)(p - 1), (byte)6);
                    return new SqlMetaData(column.ColumnName, sqlDbType, p, s);
                }

            }

            return new SqlMetaData(column.ColumnName, sqlDbType);

        }

    }
}
