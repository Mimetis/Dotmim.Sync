using Dotmim.Sync.Builders;
using Microsoft.Data.SqlClient;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer.Builders
{
    /// <summary>
    /// Sql builder for Sql Server.
    /// </summary>
    public class SqlBuilder : DbBuilder
    {
        /// <inheritdoc />
        public override async Task EnsureDatabaseAsync(DbConnection connection, DbTransaction transaction = null)
        {
            // Chek if db exists
            var exists = await SqlManagementUtils.DatabaseExistsAsync(connection as SqlConnection, transaction as SqlTransaction).ConfigureAwait(false);

            if (!exists)
                throw new MissingDatabaseException(connection.Database);
        }

        /// <inheritdoc />
        public override async Task<SyncSetup> GetAllTablesAsync(DbConnection connection, DbTransaction transaction = null)
        {
            var setup = await SqlManagementUtils.GetAllTablesAsync(connection as SqlConnection, transaction as SqlTransaction).ConfigureAwait(false);
            return setup;
        }

        /// <inheritdoc />
        public override Task<SyncTable> EnsureTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => Task.FromResult(new SyncTable(tableName, schemaName));

        /// <inheritdoc />
        public override async Task<(string DatabaseName, string Version)> GetHelloAsync(DbConnection connection, DbTransaction transaction = null)
            => await SqlManagementUtils.GetHelloAsync(connection as SqlConnection, transaction as SqlTransaction).ConfigureAwait(false);

        /// <inheritdoc />
        public override Task<SyncTable> GetTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => SqlManagementUtils.GetTableAsync(tableName, schemaName, connection as SqlConnection, transaction as SqlTransaction);

        /// <inheritdoc />
        public override Task<bool> ExistsTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => SqlManagementUtils.TableExistsAsync(tableName, schemaName, connection as SqlConnection, transaction as SqlTransaction);

        /// <inheritdoc />
        public override Task DropsTableIfExistsAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
             => SqlManagementUtils.DropTableIfExistsAsync(tableName, schemaName, connection as SqlConnection, transaction as SqlTransaction);

        /// <inheritdoc />
        public override Task RenameTableAsync(string tableName, string schemaName, string newTableName, string newSchemaName, DbConnection connection, DbTransaction transaction = null)
             => SqlManagementUtils.RenameTableAsync(tableName, schemaName, newTableName, newSchemaName, connection as SqlConnection, transaction as SqlTransaction);

        /// <inheritdoc />
        public override Task<SyncTable> GetTableDefinitionAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => SqlManagementUtils.GetTableDefinitionAsync(tableName, schemaName, connection as SqlConnection, transaction as SqlTransaction);

        /// <inheritdoc />
        public override Task<SyncTable> GetTableColumnsAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => SqlManagementUtils.GetColumnsForTableAsync(tableName, schemaName, connection as SqlConnection, transaction as SqlTransaction);
    }
}