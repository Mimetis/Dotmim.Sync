using Dotmim.Sync.Builders;
using Npgsql;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync.PostgreSql.Builders
{
    public class NpgsqlBuilder : DbBuilder
    {
        public override Task DropsTableIfExistsAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => NpgsqlManagementUtils.DropTableIfExistsAsync(connection as NpgsqlConnection, transaction as NpgsqlTransaction, tableName, schemaName);

        public override async Task EnsureDatabaseAsync(DbConnection connection, DbTransaction transaction = null)
        {
            // Chek if db exists
            var exists = await NpgsqlManagementUtils.DatabaseExistsAsync(connection as NpgsqlConnection, transaction as NpgsqlTransaction).ConfigureAwait(false);

            if (!exists)
                throw new MissingDatabaseException(connection.Database);

            var version = await NpgsqlManagementUtils.DatabaseVersionAsync(connection as NpgsqlConnection, transaction as NpgsqlTransaction).ConfigureAwait(false);

            // PostgreSQL version 15.1 supported only
            if (version < 150000)
                throw new InvalidDatabaseVersionException(version.ToString(), "PostgreSQL");
        }

        public override Task<SyncTable> EnsureTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => Task.FromResult(new SyncTable(tableName, schemaName));

        public override Task<bool> ExistsTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => NpgsqlManagementUtils.TableExistsAsync(connection as NpgsqlConnection, transaction as NpgsqlTransaction, tableName, schemaName);

        public override async Task<SyncSetup> GetAllTablesAsync(DbConnection connection, DbTransaction transaction = null)
        {
            var setup = await NpgsqlManagementUtils.GetAllTablesAsync(connection as NpgsqlConnection, transaction as NpgsqlTransaction).ConfigureAwait(false);
            return setup;
        }

        public override async Task<(string DatabaseName, string Version)> GetHelloAsync(DbConnection connection, DbTransaction transaction = null)
           => await NpgsqlManagementUtils.GetHelloAsync(connection as NpgsqlConnection, transaction as NpgsqlTransaction).ConfigureAwait(false);

        public override Task<SyncTable> GetTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => NpgsqlManagementUtils.GetTableAsync(connection as NpgsqlConnection, transaction as NpgsqlTransaction, tableName, schemaName);

        public override Task<SyncTable> GetTableColumnsAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => NpgsqlManagementUtils.GetColumnsForTableAsync(connection as NpgsqlConnection, transaction as NpgsqlTransaction, tableName, schemaName);

        public override Task<SyncTable> GetTableDefinitionAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => NpgsqlManagementUtils.GetTableDefinitionAsync(tableName, schemaName, connection as NpgsqlConnection, transaction as NpgsqlTransaction);

        public override Task RenameTableAsync(string tableName, string schemaName, string newTableName, string newSchemaName, DbConnection connection, DbTransaction transaction = null)
                            => NpgsqlManagementUtils.RenameTableAsync(tableName, schemaName, newTableName, newSchemaName, connection as NpgsqlConnection, transaction as NpgsqlTransaction);
    }
}