using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using System.Linq;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlBuilder : DbBuilder
    {
        public override async Task EnsureDatabaseAsync(DbConnection connection, DbTransaction transaction = null)
        {
            // Chek if db exists
            var exists = await SqlManagementUtils.DatabaseExistsAsync(connection as SqlConnection, transaction as SqlTransaction).ConfigureAwait(false);

            if (!exists)
                throw new MissingDatabaseException(connection.Database);

            var dbPermissions = await SqlManagementUtils.GetDatabasePermissionsAsync(connection as SqlConnection, transaction as SqlTransaction).ConfigureAwait(false);



            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            var sqlConnection = connection as SqlConnection;

            var internalInfos = sqlConnection.RetrieveInternalInfo();

            var table = sqlConnection.GetSchema();

            foreach (System.Data.DataRow row in table.Rows)
            {
                foreach (System.Data.DataColumn col in table.Columns)
                {
                    Debug.WriteLine("{0} = {1}", col.ColumnName, row[col]);
                }
                Debug.WriteLine("============================");
            }

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);


            //if (dbPermissions != null)
            //{
            //    CheckPermission(dbPermissions, "CREATE TABLE", connection);
            //    CheckPermission(dbPermissions, "CREATE PROCEDURE", connection);
            //    CheckPermission(dbPermissions, "CREATE TYPE", connection);
            //    CheckPermission(dbPermissions, "CREATE SCHEMA", connection);
            //    CheckPermission(dbPermissions, "CONNECT", connection);
            //    CheckPermission(dbPermissions, "SELECT", connection);
            //    CheckPermission(dbPermissions, "INSERT", connection);
            //    CheckPermission(dbPermissions, "UPDATE", connection);
            //    CheckPermission(dbPermissions, "DELETE", connection);
            //    CheckPermission(dbPermissions, "EXECUTE", connection);
            //    CheckPermission(dbPermissions, "VIEW DEFINITION", connection);
            //}

        }

        private void CheckPermission(List<string> permissions, string permission, DbConnection connection)
        {
            var hasPermission = permissions.Any(p => p.ToUpperInvariant() == permission);

            if (!hasPermission)
                throw new MissingDatabasePermissionException(connection.Database, permission);

        }
        public override async Task<SyncSetup> GetAllTablesAsync(DbConnection connection, DbTransaction transaction = null)
        {
            var setup = await SqlManagementUtils.GetAllTablesAsync(connection as SqlConnection, transaction as SqlTransaction).ConfigureAwait(false);
            return setup;
        }


        public override async Task<SyncTable> EnsureTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
        {
            var table = await SqlManagementUtils.GetTableDefinitionAsync(tableName, schemaName, connection as SqlConnection, transaction as SqlTransaction);

            if (table == null || !table.HasRows)
                return null;

            var tn = table.Rows[0]["TableName"].ToString();
            var sn= string.IsNullOrEmpty(table.Rows[0]["SchemaName"].ToString()) ?  null : table.Rows[0]["SchemaName"].ToString();

            var syncTable = new SyncTable(tn, sn);

            return syncTable;
        }

        public override async Task<(string DatabaseName, string Version)> GetHelloAsync(DbConnection connection, DbTransaction transaction = null) 
            => await SqlManagementUtils.GetHelloAsync(connection as SqlConnection, transaction as SqlTransaction).ConfigureAwait(false);

        /// <inheritdoc />
        public override Task<SyncTable> GetTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null) 
            => SqlManagementUtils.GetTableAsync(tableName, schemaName, connection as SqlConnection, transaction as SqlTransaction);

        public override Task<bool> ExistsTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => SqlManagementUtils.TableExistsAsync(tableName, schemaName, connection as SqlConnection, transaction as SqlTransaction);

        public override Task DropsTableIfExistsAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
             => SqlManagementUtils.DropTableIfExistsAsync(tableName, schemaName, connection as SqlConnection, transaction as SqlTransaction);

        public override Task RenameTableAsync(string tableName, string schemaName, string newTableName, string newSchemaName, DbConnection connection, DbTransaction transaction = null)
             => SqlManagementUtils.RenameTableAsync(tableName, schemaName, newTableName, newSchemaName, connection as SqlConnection, transaction as SqlTransaction);

        public override Task<SyncTable> GetTableDefinitionAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => SqlManagementUtils.GetTableDefinitionAsync(tableName, schemaName, connection as SqlConnection, transaction as SqlTransaction);


        public override Task<SyncTable> GetTableColumnsAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => SqlManagementUtils.GetColumnsForTableAsync(tableName, schemaName, connection as SqlConnection, transaction as SqlTransaction);

    }
}
