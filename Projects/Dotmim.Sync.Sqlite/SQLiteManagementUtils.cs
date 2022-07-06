
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Linq;
using Microsoft.Data.Sqlite;
using Dotmim.Sync.Builders;
using System;
using System.Data;
using System.Threading.Tasks;

namespace Dotmim.Sync.Sqlite
{
    public static class SqliteManagementUtils
    {

        /// <summary>
        /// Get all Tables
        /// </summary>
        public static async Task<SyncSetup> GetAllTablesAsync(SqliteConnection connection, SqliteTransaction transaction)
        {
            var command = $"select tbl_name from sqlite_master where type='table';";

            var syncSetup = new SyncSetup();

            using (var sqlCommand = new SqliteCommand(command, connection))
            {
                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                sqlCommand.Transaction = transaction;

                using (var reader = await sqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (reader.Read())
                    {
                        var tableName = reader.GetString(0);
                        var setupTable = new SetupTable(tableName);
                        syncSetup.Tables.Add(setupTable);
                    }
                }

                foreach (var setupTable in syncSetup.Tables)
                {
                    var syncTableColumnsList = await GetColumnsForTableAsync(connection, transaction, setupTable.TableName).ConfigureAwait(false);

                    foreach (var column in syncTableColumnsList.Rows)
                        setupTable.Columns.Add(column["name"].ToString());
                }

                if (!alreadyOpened)
                    connection.Close();

            }
            return syncSetup;
        }

        public static async Task<SyncTable> GetTableAsync(SqliteConnection connection, SqliteTransaction transaction, string unquotedTableName)
        {
            string command = "select * from sqlite_master where name = @tableName limit 1";

            var syncTable = new SyncTable(unquotedTableName);
            using (var sqlCommand = new SqliteCommand(command, connection))
            {
                sqlCommand.Parameters.AddWithValue("@tableName", unquotedTableName);

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                sqlCommand.Transaction = transaction;

                using (var reader = await sqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    syncTable.Load(reader);
                }


                if (!alreadyOpened)
                    connection.Close();

            }
            return syncTable;
        }

        public static async Task<SyncTable> GetColumnsForTableAsync(SqliteConnection connection, SqliteTransaction transaction, string unquotedTableName)
        {

            string commandColumn = $"SELECT * FROM pragma_table_info('{unquotedTableName}');";
            var syncTable = new SyncTable(unquotedTableName);

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
                    connection.Close();

            }
            return syncTable;
        }

        public static async Task<SyncTable> GetPrimaryKeysForTableAsync(SqliteConnection connection, SqliteTransaction transaction, string unquotedTableName)
        {

            string commandColumn = $"SELECT * FROM pragma_table_info('{unquotedTableName}') where pk = 1;";
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
                connection.Close();

            return syncTable;
        }

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
                    connection.Close();

            }
            return syncTable;
        }

        public static async Task DropTableIfExistsAsync(SqliteConnection connection, SqliteTransaction transaction, string quotedTableName)
        {
            var tableName = ParserName.Parse(quotedTableName).ToString();

            using (DbCommand command = connection.CreateCommand())
            {
                command.CommandText = $"drop table if exist {tableName}";
                command.Transaction = transaction;

                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        public static async Task DropTriggerIfExistsAsync(SqliteConnection connection, SqliteTransaction transaction, string quotedTriggerName)
        {
            var triggerName = ParserName.Parse(quotedTriggerName).ToString();

            using (DbCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = $"drop trigger if exist {triggerName}";
                dbCommand.Transaction = transaction;

                await dbCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        public static async Task<bool> TableExistsAsync(SqliteConnection connection, SqliteTransaction transaction, ParserName tableName)
        {
            bool tableExist;
            var quotedTableName = tableName.Unquoted().ToString();

            using (DbCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "select count(*) from sqlite_master where name = @tableName AND type='table'";

                var SqliteParameter = new SqliteParameter()
                {
                    ParameterName = "@tableName",
                    Value = quotedTableName
                };
                dbCommand.Parameters.Add(SqliteParameter);

                dbCommand.Transaction = transaction;

                tableExist = ((long)await dbCommand.ExecuteScalarAsync().ConfigureAwait(false)) != 0L;
            }
            return tableExist;
        }

        public static async Task<bool> TriggerExistsAsync(SqliteConnection connection, SqliteTransaction transaction, string quotedTriggerName)
        {
            bool triggerExist;
            var triggerName = ParserName.Parse(quotedTriggerName).ToString();

            using (var dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "select count(*) from sqlite_master where name = @triggerName AND type='trigger'";

                dbCommand.Parameters.AddWithValue("@triggerName", triggerName);

                dbCommand.Transaction = transaction;

                triggerExist = ((long)await dbCommand.ExecuteScalarAsync().ConfigureAwait(false)) != 0L;
            }
            return triggerExist;
        }

        public static string JoinOneTablesOnParametersValues(IEnumerable<string> columns, string leftName)
        {
            var stringBuilder = new StringBuilder();
            string strLeftName = (string.IsNullOrEmpty(leftName) ? string.Empty : string.Concat(leftName, "."));

            string str = "";
            foreach (var column in columns)
            {
                var quotedColumn = ParserName.Parse(column).Quoted().ToString();
                var unquotedColumn = ParserName.Parse(column).Unquoted().Normalized().ToString();


                stringBuilder.Append(str);
                stringBuilder.Append(strLeftName);
                stringBuilder.Append(quotedColumn);
                stringBuilder.Append(" = ");
                stringBuilder.Append($"@{unquotedColumn}");

                str = " AND ";
            }
            return stringBuilder.ToString();
        }

        public static string JoinTwoTablesOnClause(IEnumerable<string> columns, string leftName, string rightName)
        {
            var stringBuilder = new StringBuilder();
            string strRightName = (string.IsNullOrEmpty(rightName) ? string.Empty : string.Concat(rightName, "."));
            string strLeftName = (string.IsNullOrEmpty(leftName) ? string.Empty : string.Concat(leftName, "."));

            string str = "";
            foreach (var column in columns)
            {
                var quotedColumn = ParserName.Parse(column).Quoted().ToString();

                stringBuilder.Append(str);
                stringBuilder.Append(strLeftName);
                stringBuilder.Append(quotedColumn);
                stringBuilder.Append(" = ");
                stringBuilder.Append(strRightName);
                stringBuilder.Append(quotedColumn);

                str = " AND ";
            }
            return stringBuilder.ToString();
        }

        public static string WhereColumnAndParameters(IEnumerable<string> columns, string fromPrefix)
        {
            var stringBuilder = new StringBuilder();
            string strFromPrefix = string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, ".");
            string str1 = "";
            foreach (var column in columns)
            {
                var quotedColumn = ParserName.Parse(column).Quoted().ToString();
                var unquotedColumn = ParserName.Parse(column).Unquoted().Normalized().ToString();

                stringBuilder.Append(str1);
                stringBuilder.Append(strFromPrefix);
                stringBuilder.Append(quotedColumn);
                stringBuilder.Append(" = ");
                stringBuilder.Append($"@{unquotedColumn}");
                str1 = " AND ";
            }
            return stringBuilder.ToString();
        }

        public static string WhereColumnIsNull(IEnumerable<string> columns, string fromPrefix)
        {
            var stringBuilder = new StringBuilder();
            string strFromPrefix = string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, ".");
            string str1 = "";
            foreach (var column in columns)
            {
                var quotedColumn = ParserName.Parse(column).Quoted().ToString();

                stringBuilder.Append(str1);
                stringBuilder.Append(strFromPrefix);
                stringBuilder.Append(quotedColumn);
                stringBuilder.Append(" IS NULL ");
                str1 = " AND ";
            }
            return stringBuilder.ToString();
        }

        public static string CommaSeparatedUpdateFromParameters(SyncTable table, string fromPrefix = "")
        {
            var stringBuilder = new StringBuilder();
            string strFromPrefix = (string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, "."));
            string strSeparator = "";
            foreach (var mutableColumn in table.GetMutableColumns())
            {
                var quotedColumn = ParserName.Parse(mutableColumn).Quoted().ToString();
                var unquotedColumn = ParserName.Parse(mutableColumn).Unquoted().Normalized().ToString();

                stringBuilder.AppendLine($"{strSeparator} {strFromPrefix}{quotedColumn} = @{unquotedColumn}");
                strSeparator = ", ";
            }
            return stringBuilder.ToString();

        }
    }
}
