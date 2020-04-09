using Dotmim.Sync.Builders;

using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer.ChangeTracking
{
    public static class SqlChangeTrackingManagementUtils
    {


        /// <summary>
        /// Get Table
        /// </summary>
        public static async Task<SyncTable> ChangeTrackingTableAsync(SqlConnection connection, SqlTransaction transaction, string tableName, string schemaName)
        {
            var command = $"Select top 1 tbl.name as TableName, " +
                          $"sch.name as SchemaName " +
                          $"  from sys.change_tracking_tables tr " +
                          $"  Inner join sys.tables as tbl on tbl.object_id = tr.object_id " +
                          $"  Inner join sys.schemas as sch on tbl.schema_id = sch.schema_id " +
                          $"  Where tbl.name = @tableName and sch.name = @schemaName ";

            var tableNameNormalized = ParserName.Parse(tableName).Unquoted().Normalized().ToString();
            var tableNameString = ParserName.Parse(tableName).ToString();

            var schemaNameString = "dbo";
            if (!string.IsNullOrEmpty(schemaName))
            {
                schemaNameString = ParserName.Parse(schemaName).ToString();
                schemaNameString = string.IsNullOrWhiteSpace(schemaNameString) ? "dbo" : schemaNameString;
            }

            var syncTable = new SyncTable(tableNameNormalized, schemaNameString);
            using (var sqlCommand = new SqlCommand(command, connection))
            {
                sqlCommand.Parameters.AddWithValue("@tableName", tableNameString);
                sqlCommand.Parameters.AddWithValue("@schemaName", schemaNameString);

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                if (transaction != null)
                    sqlCommand.Transaction = transaction;

                using (var reader = await sqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    syncTable.Load(reader);
                }

                if (!alreadyOpened)
                    connection.Close();
            }
            return syncTable;
        }
    }
}
