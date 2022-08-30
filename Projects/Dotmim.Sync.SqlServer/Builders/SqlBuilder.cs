using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

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
        }

        public override async Task<SyncSetup> GetAllTablesAsync(DbConnection connection, DbTransaction transaction = null)
        {
            var setup = await SqlManagementUtils.GetAllTablesAsync(connection as SqlConnection, transaction as SqlTransaction).ConfigureAwait(false);
            return setup;
        }


        public override Task<SyncTable> EnsureTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => Task.FromResult(new SyncTable(tableName, schemaName));

        public override async Task<(string DatabaseName, string Version)> GetHelloAsync(DbConnection connection, DbTransaction transaction = null)
        {
            return await SqlManagementUtils.GetHelloAsync(connection as SqlConnection, transaction as SqlTransaction).ConfigureAwait(false);

        }

        /// <inheritdoc />
        public override Task<SyncTable> GetTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null) 
            => SqlManagementUtils.GetTableAsync(tableName, schemaName, connection as SqlConnection, transaction as SqlTransaction);

        public override Task<bool> ExistsTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => SqlManagementUtils.TableExistsAsync(tableName, schemaName, connection as SqlConnection, transaction as SqlTransaction);

        public override Task DropsTableIfExistsAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
             => SqlManagementUtils.DropTableIfExistsAsync(tableName, schemaName, connection as SqlConnection, transaction as SqlTransaction);

        public override Task RenameTableAsync(string tableName, string schemaName, string newTableName, string newSchemaName, DbConnection connection, DbTransaction transaction = null)
             => SqlManagementUtils.RenameTableAsync(tableName, schemaName, newTableName, newSchemaName, connection as SqlConnection, transaction as SqlTransaction);
    }
}
