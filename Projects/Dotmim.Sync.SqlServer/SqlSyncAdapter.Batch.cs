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
        public override async Task ExecuteBatchCommandAsync(SyncContext context, DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> applyRows, SyncTable schemaChangesTable,
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

                        // metadatas don't have readonly values, so get from sqlMetadataIndex
                        var sqlMetadataType = metadatas[sqlMetadataIndex].SqlDbType;

                        dynamic rowValue = SetRowValue(row, i, sqlMetadataType);

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
                sqlParameters["@sync_min_timestamp"].Value = lastTimestamp.HasValue ? lastTimestamp.Value : DBNull.Value;

            if (sqlParameters.Contains("@sync_force_write"))
                sqlParameters["@sync_force_write"].Value = context.SyncType == SyncType.Reinitialize || context.SyncType == SyncType.ReinitializeWithUpload ? 1 : 0;

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
                    var failedRow = new SyncRow(schemaChangesTable, syncRowState);

                    for (var i = 0; i < dataReader.FieldCount; i++)
                    {
                        var columnValueObject = dataReader.GetValue(i);
                        var columnName = dataReader.GetName(i);

                        failedRow[columnName] = columnValueObject == DBNull.Value ? null : columnValueObject;
                    }

                    // don't care about row state 
                    // Since it will be requested by next request from GetConflict()
                    failedRows.Rows.Add(failedRow);
                }

                dataReader.Close();
            }
            finally
            {
                records.Clear();

                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();
            }
        }

        private static dynamic SetRowValue(SyncRow row, int i, SqlDbType sqlMetadataType)
        {
            dynamic rowValue = row[i];

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
                        rowValue = SyncTypeConverter.TryConvertTo<DateTime>(rowValue);
                        if (sqlMetadataType == SqlDbType.DateTime && rowValue < SqlDateMin)
                            rowValue = SqlDateMin;
                        else if (sqlMetadataType == SqlDbType.SmallDateTime && rowValue < SqlSmallDateMin)
                            rowValue = SqlSmallDateMin;
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

            return rowValue ?? DBNull.Value;
        }

        private SqlMetaData GetSqlMetadaFromType(SyncColumn column)
        {
            long maxLength = column.MaxLength;

            var sqlDbType = GetSqlDbType(column);

            // Since we validate length before, it's not mandatory here.
            // let's say.. just in case..
            switch (sqlDbType)
            {
                case SqlDbType.NVarChar:
                    maxLength = maxLength <= 0 ? SqlMetaData.Max : Math.Min(maxLength, 4000);
                    break;
                case SqlDbType.VarChar:
                case SqlDbType.VarBinary:
                    maxLength = maxLength <= 0 ? SqlMetaData.Max : Math.Min(maxLength, 8000);
                    break;
                case SqlDbType.NChar:
                    maxLength = maxLength <= 0 ? 4000 : maxLength;
                    break;
                case SqlDbType.Char:
                case SqlDbType.Binary:
                    maxLength = maxLength <= 0 ? 8000 : maxLength;
                    break;
                case SqlDbType.Decimal:
                    var (p, s) = this.SqlMetadata.GetPrecisionAndScale(column);
                    if (p <= 0 || p <= s)
                    {
                        if (p == 0)
                            p = 18;
                        if (s == 0)
                            s = Math.Min((byte)(p - 1), (byte)6);
                    }
                    return new SqlMetaData(column.ColumnName, sqlDbType, p, s);
                default:
                    var dataType = column.GetDataType();

                    if (dataType != typeof(char))
                    {
                        return new SqlMetaData(column.ColumnName, sqlDbType);
                    }

                    maxLength = 1;
                    break;
            }

            return new SqlMetaData(column.ColumnName, sqlDbType, maxLength);
        }

        private SqlDbType GetSqlDbType(SyncColumn column)
        {
            // TODO : Find something better than string comparison for change tracking provider
            var isSameProvider = this.TableDescription.OriginalProvider == SqlSyncProvider.ProviderType ||
            this.TableDescription.OriginalProvider == "SqlSyncChangeTrackingProvider, Dotmim.Sync.SqlServer.SqlSyncChangeTrackingProvider";

            if (isSameProvider)
                return this.SqlMetadata.GetSqlDbType(column);

            return this.SqlMetadata.GetOwnerDbTypeFromDbType(column);
        }
    }
}
