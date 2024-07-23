using Dotmim.Sync.Builders;
#if NETCOREAPP
using MySqlConnector;
#elif NETSTANDARD
using MySql.Data.MySqlClient;
#endif
using System.Data.Common;
using System.Threading.Tasks;

#if MARIADB
namespace Dotmim.Sync.MariaDB.Builders
#elif MYSQL
namespace Dotmim.Sync.MySql.Builders
#endif
{
    /// <inheritdoc />
    public class MySqlBuilder : DbBuilder
    {
        public override Task EnsureDatabaseAsync(DbConnection connection, DbTransaction transaction = null)
        {
            return Task.CompletedTask;

            // using var dbCommand = connection.CreateCommand();
            // dbCommand.CommandText = $"set global innodb_stats_on_metadata=0;";

            // bool alreadyOpened = connection.State == ConnectionState.Open;

            // if (!alreadyOpened)
            //    await connection.OpenAsync().ConfigureAwait(false);

            // dbCommand.Transaction = transaction;

            // await dbCommand.ExecuteNonQueryAsync().ConfigureAwait(false);

            // if (!alreadyOpened)
            //    connection.Close();
        }

        public override async Task<SyncSetup> GetAllTablesAsync(DbConnection connection, DbTransaction transaction = null)
        {
            var setup = await MySqlManagementUtils.GetAllTablesAsync(connection as MySqlConnection, transaction as MySqlTransaction).ConfigureAwait(false);
            return setup;
        }

        public override Task<SyncTable> EnsureTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => Task.FromResult(new SyncTable(tableName));

        public override async Task<(string DatabaseName, string Version)> GetHelloAsync(DbConnection connection, DbTransaction transaction = null)
        {
            return await MySqlManagementUtils.GetHelloAsync(connection as MySqlConnection, transaction as MySqlTransaction).ConfigureAwait(false);
        }

        public override Task<SyncTable> GetTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
              => MySqlManagementUtils.GetTableAsync(tableName, connection as MySqlConnection, transaction as MySqlTransaction);

        public override Task<bool> ExistsTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
             => MySqlManagementUtils.TableExistsAsync(tableName, connection as MySqlConnection, transaction as MySqlTransaction);

        public override Task DropsTableIfExistsAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
             => MySqlManagementUtils.DropTableIfExistsAsync(tableName, connection as MySqlConnection, transaction as MySqlTransaction);

        public override Task RenameTableAsync(string tableName, string schemaName, string newTableName, string newSchemaName, DbConnection connection, DbTransaction transaction = null)
             => MySqlManagementUtils.RenameTableAsync(tableName, newTableName, connection as MySqlConnection, transaction as MySqlTransaction);

        public override Task<SyncTable> GetTableDefinitionAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
              => MySqlManagementUtils.GetTableDefinitionAsync(tableName, connection as MySqlConnection, transaction as MySqlTransaction);

        public override Task<SyncTable> GetTableColumnsAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
              => MySqlManagementUtils.GetColumnsForTableAsync(tableName, connection as MySqlConnection, transaction as MySqlTransaction);
    }
}