using Dotmim.Sync.Builders;
using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Sqlite.Builders
{
    public class SqliteBuilder : DbBuilder
    {
        public override Task EnsureDatabaseAsync(DbConnection connection, DbTransaction transaction = null)
            => Task.CompletedTask;

        public override Task<SyncTable> EnsureTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => Task.FromResult(new SyncTable(tableName));

        public override async Task<SyncSetup> GetAllTablesAsync(DbConnection connection, DbTransaction transaction = null)
        {
            var setup = await SqliteManagementUtils.GetAllTablesAsync(connection as SqliteConnection, transaction as SqliteTransaction).ConfigureAwait(false);
            return setup;
        }

        public override Task<(string DatabaseName, string Version)> GetHelloAsync(DbConnection connection, DbTransaction transaction = null)
            => throw new NotImplementedException();
    }
}
