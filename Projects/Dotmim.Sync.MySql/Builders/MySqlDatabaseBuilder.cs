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

    /// <summary>
    /// Represents a MySql database builder.
    /// </summary>
    public class MySqlDatabaseBuilder : DbDatabaseBuilder
    {
        /// <inheritdoc/>
        public override Task EnsureDatabaseAsync(DbConnection connection, DbTransaction transaction = null) => Task.CompletedTask;

        /// <inheritdoc/>
        public override Task<SyncSetup> GetAllTablesAsync(DbConnection connection, DbTransaction transaction = null)
            => MySqlManagementUtils.GetAllTablesAsync(connection as MySqlConnection, transaction as MySqlTransaction);

        /// <inheritdoc/>
        public override Task<SyncTable> EnsureTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => Task.FromResult(new SyncTable(tableName));

        /// <inheritdoc/>
        public override Task<(string DatabaseName, string Version)> GetHelloAsync(DbConnection connection, DbTransaction transaction = null)
            => MySqlManagementUtils.GetHelloAsync(connection as MySqlConnection, transaction as MySqlTransaction);

        /// <inheritdoc/>
        public override Task<SyncTable> GetTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
              => MySqlManagementUtils.GetTableAsync(tableName, connection as MySqlConnection, transaction as MySqlTransaction);

        /// <inheritdoc/>
        public override Task<bool> ExistsTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
             => MySqlManagementUtils.TableExistsAsync(tableName, connection as MySqlConnection, transaction as MySqlTransaction);

        /// <inheritdoc/>
        public override Task DropsTableIfExistsAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
             => MySqlManagementUtils.DropTableIfExistsAsync(tableName, connection as MySqlConnection, transaction as MySqlTransaction);

        /// <inheritdoc/>
        public override Task RenameTableAsync(string tableName, string schemaName, string newTableName, string newSchemaName, DbConnection connection, DbTransaction transaction = null)
             => MySqlManagementUtils.RenameTableAsync(tableName, newTableName, connection as MySqlConnection, transaction as MySqlTransaction);
    }
}