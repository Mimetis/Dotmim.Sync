using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;
using Npgsql;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync.PostgreSql.Builders
{
    /// <summary>
    /// Represents a database builder for Npgsql.
    /// </summary>
    public class NpgsqlDatabaseBuilder : DbDatabaseBuilder
    {
        /// <summary>
        /// Drops a table if exists.
        /// </summary>
        public override Task DropsTableIfExistsAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
        {
            var tableParser = new TableParser($"{tableName}.{schemaName}", NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
            return NpgsqlManagementUtils.DropTableIfExistsAsync(connection as NpgsqlConnection, transaction as NpgsqlTransaction,
                tableParser.TableName, tableParser.SchemaName);
        }

        /// <summary>
        /// Ensure the database exists and is ready for sync.
        /// </summary>
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

        /// <inheritdoc/>
        public override Task<SyncTable> EnsureTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => Task.FromResult(new SyncTable(tableName, schemaName));

        /// <inheritdoc/>
        public override Task<bool> ExistsTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
        {
            var tableParser = new TableParser($"{tableName}.{schemaName}", NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
            return NpgsqlManagementUtils.TableExistsAsync(connection as NpgsqlConnection, transaction as NpgsqlTransaction,
                tableParser.TableName, tableParser.SchemaName);
        }

        /// <inheritdoc/>
        public override Task<SyncSetup> GetAllTablesAsync(DbConnection connection, DbTransaction transaction = null)
            => NpgsqlManagementUtils.GetAllTablesAsync(connection as NpgsqlConnection, transaction as NpgsqlTransaction);

        /// <inheritdoc/>
        public override async Task<(string DatabaseName, string Version)> GetHelloAsync(DbConnection connection, DbTransaction transaction = null)
           => await NpgsqlManagementUtils.GetHelloAsync(connection as NpgsqlConnection, transaction as NpgsqlTransaction).ConfigureAwait(false);

        /// <inheritdoc/>
        public override Task<SyncTable> GetTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
        {
            var tableParser = new TableParser($"{tableName}.{schemaName}", NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
            return NpgsqlManagementUtils.GetTableAsync(connection as NpgsqlConnection, transaction as NpgsqlTransaction,
                tableParser.TableName, tableParser.SchemaName);
        }

        /// <inheritdoc/>
        public override Task RenameTableAsync(string tableName, string schemaName, string newTableName, string newSchemaName, DbConnection connection, DbTransaction transaction = null)
        {
            var tableParser = new TableParser($"{tableName}.{schemaName}", NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
            var newTableParser = new TableParser($"{newTableName}.{newSchemaName}", NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);

            return NpgsqlManagementUtils.RenameTableAsync(tableParser.TableName, tableParser.SchemaName,
                newTableParser.TableName, newTableParser.SchemaName, connection as NpgsqlConnection, transaction as NpgsqlTransaction);
        }
    }
}