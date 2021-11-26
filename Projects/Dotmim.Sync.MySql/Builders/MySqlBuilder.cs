using Dotmim.Sync.Builders;
#if NETCOREAPP
using MySqlConnector;
#elif NETSTANDARD 
using MySql.Data.MySqlClient;
#endif
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

#if MARIADB
namespace Dotmim.Sync.MariaDB.Builders
#elif MYSQL
namespace Dotmim.Sync.MySql.Builders
#endif
{
#if MARIADB
    public class MariaDBBuilder : DbBuilder
#elif MYSQL
    public class MySqlBuilder : DbBuilder
#endif
    {
        public override async Task EnsureDatabaseAsync(DbConnection connection, DbTransaction transaction = null)
        {
            using var dbCommand = connection.CreateCommand();
            dbCommand.CommandText = $"set global innodb_stats_on_metadata=0;";

            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            dbCommand.Transaction = transaction;

            await dbCommand.ExecuteNonQueryAsync().ConfigureAwait(false);

            if (!alreadyOpened)
                connection.Close();
        }

        public override Task<SyncTable> EnsureTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => Task.FromResult(new SyncTable(tableName));

        public override async Task<(string DatabaseName, string Version)> GetHelloAsync(DbConnection connection, DbTransaction transaction = null)
        {
#if MARIADB
            return await MariaDBManagementUtils.GetHelloAsync(connection as MySqlConnection, transaction as MySqlTransaction).ConfigureAwait(false);
#elif MYSQL
            return await MySqlManagementUtils.GetHelloAsync(connection as MySqlConnection, transaction as MySqlTransaction).ConfigureAwait(false);
#endif
        }
    }
}
