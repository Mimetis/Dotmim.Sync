using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;

#if MARIADB
using Dotmim.Sync.MariaDB.Builders;
#elif MYSQL
using Dotmim.Sync.MySql.Builders;
#endif

#if NET6_0 || NET8_0
using MySqlConnector;
#elif NETSTANDARD
using MySql.Data.MySqlClient;
#endif

using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

#if MARIADB
namespace Dotmim.Sync.MariaDB
#elif MYSQL
namespace Dotmim.Sync.MySql
#endif
{

    /// <summary>
    /// MySql Management utils.
    /// </summary>
    public static class MySqlManagementUtils
    {

        /// <summary>
        /// check a schema and version. health check.
        /// </summary>
        public static async Task<(string DatabaseName, string EngineVersion)> GetHelloAsync(MySqlConnection connection, MySqlTransaction transaction)
        {
            string dbName = null;
            string dbVersion = null;

            using (DbCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "select schema_name, version() as version from information_schema.schemata where schema_name=@databaseName;";

                var sqlParameter = new MySqlParameter()
                {
                    ParameterName = "@databaseName",
                    Value = connection.Database,
                };
                dbCommand.Parameters.Add(sqlParameter);

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                dbCommand.Transaction = transaction;

                using (var reader = await dbCommand.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    if (reader.HasRows)
                    {
                        await reader.ReadAsync().ConfigureAwait(false);
                        dbName = reader.GetString(0);
                        dbVersion = reader.GetString(1);
                    }
                }

                if (!alreadyOpened)
                    await connection.CloseAsync().ConfigureAwait(false);
            }

            return (dbName, dbVersion);
        }

        /// <summary>
        /// Get all Tables.
        /// </summary>
        public static async Task<SyncSetup> GetAllTablesAsync(MySqlConnection connection, MySqlTransaction transaction)
        {
            var command = $"select TABLE_NAME from information_schema.TABLES where table_schema = schema()";

            var syncSetup = new SyncSetup();

            using (var mySqlCommand = new MySqlCommand(command, connection))
            {
                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                mySqlCommand.Transaction = transaction;

                using (var reader = await mySqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        var tableName = reader.GetString(0);
                        var setupTable = new SetupTable(tableName);
                        syncSetup.Tables.Add(setupTable);
                    }
                }

                foreach (var setupTable in syncSetup.Tables)
                {
                    var syncTableColumnsList = await GetColumnsForTableAsync(setupTable.TableName, connection, transaction).ConfigureAwait(false);

                    foreach (var column in syncTableColumnsList.Rows)
                        setupTable.Columns.Add(column["column_name"].ToString());
                }

                if (!alreadyOpened)
                    await connection.CloseAsync().ConfigureAwait(false);
            }

            return syncSetup;
        }

        /// <summary>
        /// Get all rows from a table.
        /// </summary>
        public static async Task<SyncTable> GetTableAsync(string tableName, MySqlConnection connection, MySqlTransaction transaction)
        {
            var syncTable = new SyncTable(tableName);

            string commandText = $"select * from {tableName}";

            using var sqlCommand = new MySqlCommand(commandText, connection);

            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            sqlCommand.Transaction = transaction;

            using (var reader = await sqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
                syncTable.Load(reader);

            if (!alreadyOpened)
                await connection.CloseAsync().ConfigureAwait(false);

            return syncTable;
        }

        /// <summary>
        /// Rename a table.
        /// </summary>
        public static async Task RenameTableAsync(string tableName, string newTableName, MySqlConnection connection, MySqlTransaction transaction)
        {
            var commandText = $"RENAME TABLE {tableName} TO {tableName};";

            using var sqlCommand = new MySqlCommand(commandText, connection);

            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            sqlCommand.Transaction = transaction;

            await sqlCommand.ExecuteNonQueryAsync().ConfigureAwait(false);

            if (!alreadyOpened)
                await connection.CloseAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Get columns for a table.
        /// </summary>
        public static async Task<SyncTable> GetColumnsForTableAsync(string tableName, MySqlConnection connection, MySqlTransaction transaction)
        {
            string commandColumn = "select * from information_schema.COLUMNS where table_schema = schema() and table_name = @tableName";

            var syncTable = new SyncTable(tableName);
            using (var sqlCommand = new MySqlCommand(commandColumn, connection))
            {
                sqlCommand.Parameters.AddWithValue("@tableName", tableName);

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                sqlCommand.Transaction = transaction;

                using (var reader = await sqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    syncTable.Load(reader);
                }

                if (!alreadyOpened)
                    await connection.CloseAsync().ConfigureAwait(false);
            }

            return syncTable;
        }

        /// <summary>
        /// Drop a table if exists.
        /// </summary>
        public static async Task DropTableIfExistsAsync(string tableName, MySqlConnection connection, MySqlTransaction transaction)
        {

            using var dbCommand = connection.CreateCommand();
            dbCommand.CommandText = $"select * from information_schema.TABLES where table_schema = schema() and table_name = @tableName";

            dbCommand.Parameters.AddWithValue("@tableName", tableName);

            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            dbCommand.Transaction = transaction;

            await dbCommand.ExecuteNonQueryAsync().ConfigureAwait(false);

            if (!alreadyOpened)
                await connection.CloseAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Check if a table exists.
        /// </summary>
        public static async Task<bool> TableExistsAsync(string tableName, MySqlConnection connection, MySqlTransaction transaction)
        {
            bool tableExist;

            using DbCommand dbCommand = connection.CreateCommand();

            dbCommand.CommandText = "select COUNT(*) from information_schema.TABLES where TABLE_NAME = @tableName and TABLE_SCHEMA = schema() and TABLE_TYPE = 'BASE TABLE'";

            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            dbCommand.Transaction = transaction;

            var sqlParameter = new MySqlParameter()
            {
                ParameterName = "@tableName",
                Value = tableName,
            };

            dbCommand.Parameters.Add(sqlParameter);

            tableExist = ((long)await dbCommand.ExecuteScalarAsync().ConfigureAwait(false)) != 0;

            if (!alreadyOpened)
                await connection.CloseAsync().ConfigureAwait(false);

            return tableExist;
        }

        /// <summary>
        /// Join two tables on clause.
        /// </summary>
        public static string JoinTwoTablesOnClause(IEnumerable<SyncColumn> pkeys, string leftName, string rightName)
        {
            var stringBuilder = new StringBuilder();
            string strRightName = string.IsNullOrEmpty(rightName) ? string.Empty : string.Concat(rightName, ".");
            string strLeftName = string.IsNullOrEmpty(leftName) ? string.Empty : string.Concat(leftName, ".");

            string str = string.Empty;
            foreach (var column in pkeys)
            {
                var quotedColumnParser = new ObjectParser(column.ColumnName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);

                stringBuilder.Append(str);
                stringBuilder.Append(strLeftName);
                stringBuilder.Append(quotedColumnParser.QuotedShortName);
                stringBuilder.Append(" = ");
                stringBuilder.Append(strRightName);
                stringBuilder.Append(quotedColumnParser.QuotedShortName);

                str = " AND ";
            }

            return stringBuilder.ToString();
        }

        /// <summary>
        /// Where clause for columns.
        /// </summary>
        public static string WhereColumnAndParameters(IEnumerable<SyncColumn> primaryKeys, string fromPrefix, string mysqlPrefix = MySqlBuilderProcedure.MYSQLPREFIXPARAMETER)
        {
            StringBuilder stringBuilder = new StringBuilder();
            string strFromPrefix = string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, ".");
            string str1 = string.Empty;
            foreach (var column in primaryKeys)
            {
                var quotedColumnParser = new ObjectParser(column.ColumnName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);
                var paramQuotedColumnParser = new ObjectParser($"{mysqlPrefix}{column.ColumnName}", MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);

                stringBuilder.Append(str1);
                stringBuilder.Append(strFromPrefix);
                stringBuilder.Append(quotedColumnParser.QuotedShortName);
                stringBuilder.Append(" = ");
                stringBuilder.Append($"{paramQuotedColumnParser.QuotedShortName}");
                str1 = " AND ";
            }

            return stringBuilder.ToString();
        }

        /// <summary>
        /// Comma separated columns.
        /// </summary>
        public static string CommaSeparatedUpdateFromParameters(SyncTable table, string fromPrefix = "", string mysqlPrefix = MySqlBuilderProcedure.MYSQLPREFIXPARAMETER)
        {
            var stringBuilder = new StringBuilder();
            string strFromPrefix = string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, ".");
            string strSeparator = string.Empty;
            foreach (var mutableColumn in table.GetMutableColumns())
            {
                var quotedColumnParser = new ObjectParser(mutableColumn.ColumnName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);
                var paramQuotedColumnParser = new ObjectParser($"{mysqlPrefix}{mutableColumn.ColumnName}", MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);
                stringBuilder.AppendLine($"{strSeparator} {strFromPrefix}{quotedColumnParser.QuotedShortName} = {MySqlObjectNames.LeftQuote}{paramQuotedColumnParser.NormalizedShortName}{MySqlObjectNames.RightQuote}");
                strSeparator = ", ";
            }

            return stringBuilder.ToString();
        }
    }
}