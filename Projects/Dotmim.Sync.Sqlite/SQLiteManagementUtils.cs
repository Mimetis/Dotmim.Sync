using Dotmim.Sync.DatabaseStringParsers;
using Microsoft.Data.Sqlite;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Sqlite
{
    /// <summary>
    /// Sqlite Management Utils.
    /// </summary>
    public static class SqliteManagementUtils
    {

        /// <summary>
        /// Get all Tables.
        /// </summary>
        public static async Task<SyncSetup> GetAllTablesAsync(SqliteConnection connection, SqliteTransaction transaction)
        {
            var command = $"select tbl_name from sqlite_master where type='table' and tbl_name not like 'sqlite_%';";

            var syncSetup = new SyncSetup();

            using (var sqlCommand = new SqliteCommand(command, connection))
            {
                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                sqlCommand.Transaction = transaction;

                using (var reader = await sqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
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
                        setupTable.Columns.Add(column["name"].ToString());
                }

                if (!alreadyOpened)
#if NET6_0_OR_GREATER
                    await connection.CloseAsync().ConfigureAwait(false);
#else
                    connection.Close();
#endif
            }

            return syncSetup;
        }

        /// <summary>
        /// Get all rows from a table.
        /// </summary>
        public static async Task<SyncTable> GetTableAsync(string quotedTableName, SqliteConnection connection, SqliteTransaction transaction)
        {
            var command = $"Select * from {quotedTableName};";

            var syncTable = new SyncTable(quotedTableName);

            using var sqlCommand = new SqliteCommand(command, connection);

            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            sqlCommand.Transaction = transaction;

            using (var reader = await sqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
                syncTable.Load(reader);

            if (!alreadyOpened)
#if NET6_0_OR_GREATER
                await connection.CloseAsync().ConfigureAwait(false);
#else
                connection.Close();
#endif

            return syncTable;
        }

        /// <summary>
        /// Rename a table.
        /// </summary>
        public static async Task RenameTableAsync(string tableName, string newTableName, SqliteConnection connection, SqliteTransaction transaction)
        {
            var commandText = $"ALTER TABLE [{tableName}] RENAME TO {newTableName};";

            using var sqlCommand = new SqliteCommand(commandText, connection);

            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            sqlCommand.Transaction = transaction;

            await sqlCommand.ExecuteNonQueryAsync().ConfigureAwait(false);

            if (!alreadyOpened)
#if NET6_0_OR_GREATER
                await connection.CloseAsync().ConfigureAwait(false);
#else
                connection.Close();
#endif
        }

        /// <summary>
        /// Get all columns for a table.
        /// </summary>
        public static async Task<SyncTable> GetColumnsForTableAsync(string tableName, SqliteConnection connection, SqliteTransaction transaction)
        {
            string commandColumn = $"SELECT * FROM pragma_table_info('{tableName}');";
            var syncTable = new SyncTable(tableName);

            using (var sqlCommand = new SqliteCommand(commandColumn, connection))
            {
                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                sqlCommand.Transaction = transaction;

                using (var reader = await sqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    syncTable.Load(reader);
                }

                if (!alreadyOpened)
#if NET6_0_OR_GREATER
                    await connection.CloseAsync().ConfigureAwait(false);
#else
                    connection.Close();
#endif
            }

            return syncTable;
        }

        /// <summary>
        /// Get all primary keys for a table.
        /// </summary>
        public static async Task<SyncTable> GetPrimaryKeysForTableAsync(SqliteConnection connection, SqliteTransaction transaction, string unquotedTableName)
        {

            string commandColumn = $"SELECT * FROM pragma_table_info('{unquotedTableName}') where pk <> 0 order by pk asc;";
            var syncTable = new SyncTable(unquotedTableName);

            using var sqlCommand = new SqliteCommand(commandColumn, connection);

            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            sqlCommand.Transaction = transaction;

            using (var reader = await sqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
            {
                syncTable.Load(reader);
            }

            if (!alreadyOpened)
#if NET6_0_OR_GREATER
                await connection.CloseAsync().ConfigureAwait(false);
#else
                connection.Close();
#endif

            return syncTable;
        }

        /// <summary>
        /// Get relations for a table.
        /// </summary>
        public static async Task<SyncTable> GetRelationsForTableAsync(SqliteConnection connection, SqliteTransaction transaction, string unqotedTableName)
        {
            string commandColumn = $"SELECT * FROM pragma_foreign_key_list('{unqotedTableName}')";
            var syncTable = new SyncTable(unqotedTableName);

            using (var sqlCommand = new SqliteCommand(commandColumn, connection))
            {
                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                sqlCommand.Transaction = transaction;

                using (var reader = await sqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    syncTable.Load(reader);
                }

                if (!alreadyOpened)
#if NET6_0_OR_GREATER
                    await connection.CloseAsync().ConfigureAwait(false);
#else
                    connection.Close();
#endif
            }

            return syncTable;
        }

        /// <summary>
        /// Drop a table if it exists.
        /// </summary>
        public static async Task DropTableIfExistsAsync(string tableName, SqliteConnection connection, SqliteTransaction transaction)
        {
            using DbCommand command = connection.CreateCommand();
            command.CommandText = $"drop table if exist {tableName}";
            command.Transaction = transaction;

            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Drop a trigger if it exists.
        /// </summary>
        public static async Task DropTriggerIfExistsAsync(SqliteConnection connection, SqliteTransaction transaction, string quotedTriggerName)
        {
            using DbCommand dbCommand = connection.CreateCommand();
            dbCommand.CommandText = $"drop trigger if exist {quotedTriggerName}";
            dbCommand.Transaction = transaction;

            await dbCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Check if a table exists.
        /// </summary>
        public static async Task<bool> TableExistsAsync(string tableName, SqliteConnection connection, SqliteTransaction transaction)
        {
            bool tableExist;

            using (DbCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "select count(*) from sqlite_master where name = @tableName AND type='table'";

                var sqliteParameter = new SqliteParameter()
                {
                    ParameterName = "@tableName",
                    Value = tableName,
                };
                dbCommand.Parameters.Add(sqliteParameter);

                dbCommand.Transaction = transaction;

                tableExist = ((long)await dbCommand.ExecuteScalarAsync().ConfigureAwait(false)) != 0L;
            }

            return tableExist;
        }

        /// <summary>
        /// Check if a trigger exists.
        /// </summary>
        public static async Task<bool> TriggerExistsAsync(SqliteConnection connection, SqliteTransaction transaction, string quotedTriggerName)
        {
            bool triggerExist;

            using (var dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "select count(*) from sqlite_master where name = @triggerName AND type='trigger'";

                dbCommand.Parameters.AddWithValue("@triggerName", quotedTriggerName);

                dbCommand.Transaction = transaction;

                triggerExist = ((long)await dbCommand.ExecuteScalarAsync().ConfigureAwait(false)) != 0L;
            }

            return triggerExist;
        }

        /// <summary>
        /// Returns a string that joins two tables on parameters values.
        /// </summary>
        public static string JoinOneTablesOnParametersValues(IEnumerable<string> columns, string leftName)
        {
            var stringBuilder = new StringBuilder();
            string strLeftName = string.IsNullOrEmpty(leftName) ? string.Empty : string.Concat(leftName, ".");

            string str = string.Empty;
            foreach (var column in columns)
            {
                var objectParser = new ObjectParser(column, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);

                stringBuilder.Append(str);
                stringBuilder.Append(strLeftName);
                stringBuilder.Append(objectParser.QuotedShortName);
                stringBuilder.Append(" = ");
                stringBuilder.Append($"@{objectParser.NormalizedShortName}");

                str = " AND ";
            }

            return stringBuilder.ToString();
        }

        /// <summary>
        /// Returns a join table on clause.
        /// </summary>
        public static string JoinTwoTablesOnClause(IEnumerable<string> columns, string leftName, string rightName)
        {
            var stringBuilder = new StringBuilder();
            string strRightName = string.IsNullOrEmpty(rightName) ? string.Empty : string.Concat(rightName, ".");
            string strLeftName = string.IsNullOrEmpty(leftName) ? string.Empty : string.Concat(leftName, ".");

            string str = string.Empty;
            foreach (var column in columns)
            {
                var objectParser = new ObjectParser(column, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);

                stringBuilder.Append(str);
                stringBuilder.Append(strLeftName);
                stringBuilder.Append(objectParser.QuotedShortName);
                stringBuilder.Append(" = ");
                stringBuilder.Append(strRightName);
                stringBuilder.Append(objectParser.QuotedShortName);

                str = " AND ";
            }

            return stringBuilder.ToString();
        }

        /// <summary>
        /// Returns a string that joins columns on a where clause.
        /// </summary>
        public static string WhereColumnAndParameters(IEnumerable<string> columns, string fromPrefix)
        {
            var stringBuilder = new StringBuilder();
            string strFromPrefix = string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, ".");
            string str1 = string.Empty;
            foreach (var column in columns)
            {
                var objectParser = new ObjectParser(column, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);

                stringBuilder.Append(str1);
                stringBuilder.Append(strFromPrefix);
                stringBuilder.Append(objectParser.QuotedShortName);
                stringBuilder.Append(" = ");
                stringBuilder.Append($"@{objectParser.NormalizedShortName}");
                str1 = " AND ";
            }

            return stringBuilder.ToString();
        }

        /// <summary>
        /// Returns a string that joins columns on null.
        /// </summary>
        public static string WhereColumnIsNull(IEnumerable<string> columns, string fromPrefix)
        {
            var stringBuilder = new StringBuilder();
            string strFromPrefix = string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, ".");
            string str1 = string.Empty;
            foreach (var column in columns)
            {
                var objectParser = new ObjectParser(column, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);

                stringBuilder.Append(str1);
                stringBuilder.Append(strFromPrefix);
                stringBuilder.Append(objectParser.QuotedShortName);
                stringBuilder.Append(" IS NULL ");
                str1 = " AND ";
            }

            return stringBuilder.ToString();
        }
    }
}