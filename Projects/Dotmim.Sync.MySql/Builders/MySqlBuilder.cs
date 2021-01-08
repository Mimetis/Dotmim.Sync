using Dotmim.Sync.Builders;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.MySql.Builders
{
    public class MySqlBuilder : DbBuilder
    {
        public override async Task EnsureDatabaseAsync(DbConnection connection, DbTransaction transaction = null)
        {
            using var dbCommand = connection.CreateCommand();
            dbCommand.CommandText = $"set global innodb_stats_on_metadata=0;";

            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            if (transaction != null)
                dbCommand.Transaction = transaction;

            await dbCommand.ExecuteNonQueryAsync().ConfigureAwait(false);

            if (!alreadyOpened)
                connection.Close();
        }

        public override Task<SyncTable> EnsureTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => Task.FromResult(new SyncTable(tableName));

        public override async Task<(string DatabaseName, string Version)> GetHelloAsync(DbConnection connection, DbTransaction transaction = null)
        {
            return await MySqlManagementUtils.GetHelloAsync(connection as MySqlConnection, transaction as MySqlTransaction).ConfigureAwait(false);
        }
    }
}
