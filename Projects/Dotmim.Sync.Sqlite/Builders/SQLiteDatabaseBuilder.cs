using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;
using Microsoft.Data.Sqlite;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync.Sqlite.Builders
{
    /// <inheritdoc />
    public class SQLiteDatabaseBuilder : DbDatabaseBuilder
    {
        /// <inheritdoc />
        public override Task EnsureDatabaseAsync(DbConnection connection, DbTransaction transaction = null)
            => Task.CompletedTask;

        /// <inheritdoc />
        public override Task<SyncTable> EnsureTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => Task.FromResult(new SyncTable(tableName));

        /// <inheritdoc />
        public override async Task<SyncSetup> GetAllTablesAsync(DbConnection connection, DbTransaction transaction = null)
        {
            var setup = await SqliteManagementUtils.GetAllTablesAsync(connection as SqliteConnection, transaction as SqliteTransaction).ConfigureAwait(false);
            return setup;
        }

        /// <inheritdoc />
        public override Task<(string DatabaseName, string Version)> GetHelloAsync(DbConnection connection, DbTransaction transaction = null)
            => throw new NotImplementedException();

        /// <inheritdoc />
        public override Task<SyncTable> GetTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
        {
            var tableParser = new TableParser(tableName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);
            return SqliteManagementUtils.GetTableAsync(tableParser.TableName, connection as SqliteConnection, transaction as SqliteTransaction);
        }

        /// <inheritdoc />
        public override Task<bool> ExistsTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
        {
            var tableParser = new TableParser(tableName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);
            return SqliteManagementUtils.TableExistsAsync(tableParser.TableName, connection as SqliteConnection, transaction as SqliteTransaction);
        }

        /// <inheritdoc />
        public override Task DropsTableIfExistsAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
        {
            var tableParser = new TableParser(tableName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);
            return SqliteManagementUtils.DropTableIfExistsAsync(tableParser.TableName, connection as SqliteConnection, transaction as SqliteTransaction);
        }

        /// <inheritdoc />
        public override Task RenameTableAsync(string tableName, string schemaName, string newTableName, string newSchemaName, DbConnection connection, DbTransaction transaction = null)
        {
            var tableParser = new TableParser(tableName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);
            var newTableParser = new TableParser(newTableName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);
            return SqliteManagementUtils.RenameTableAsync(tableParser.TableName, newTableParser.TableName, connection as SqliteConnection, transaction as SqliteTransaction);
        }
    }
}