using Dotmim.Sync.Builders;
#if MARIADB
using Dotmim.Sync.MariaDB.Builders;
#elif MYSQL
using Dotmim.Sync.MySql.Builders;
#endif

#if NET5_0 || NET6_0 || NET7_0 || NETCOREAPP3_1
using MySqlConnector;
#elif NETSTANDARD
using MySql.Data.MySqlClient;
#endif

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

#if MARIADB
namespace Dotmim.Sync.MariaDB
#elif MYSQL
namespace Dotmim.Sync.MySql
#endif
{
    public static class MySqlManagementUtils
    {

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
                    Value = connection.Database
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
                        reader.Read();
                        dbName = reader.GetString(0);
                        dbVersion = reader.GetString(1);
                    }
                }

                if (!alreadyOpened)
                    connection.Close();
            }
            return (dbName, dbVersion);
        }

        /// <summary>
        /// Get all Tables
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
                    while (reader.Read())
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
                    connection.Close();
            }
            return syncSetup;
        }

        public static async Task<SyncTable> GetTableAsync(string tableName, MySqlConnection connection, MySqlTransaction transaction)
        {

            var tableNameParser = ParserName.Parse(tableName, "`");
            var syncTable = new SyncTable(tableNameParser.Unquoted().ToString());

            string commandText = $"select * from {tableNameParser.Quoted()}";

            using var sqlCommand = new MySqlCommand(commandText, connection);

            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            sqlCommand.Transaction = transaction;

            using (var reader = await sqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
                syncTable.Load(reader);

            if (!alreadyOpened)
                connection.Close();

            return syncTable;
        }


        public static async Task RenameTableAsync(string tableName, string newTableName, MySqlConnection connection, MySqlTransaction transaction)
        {
            var pTableName = ParserName.Parse(tableName, "`").Unquoted().ToString();
            var pNewTableName = ParserName.Parse(newTableName, "`").Unquoted().ToString();

            var commandText = $"RENAME TABLE {pTableName} TO {pNewTableName};";

            using var sqlCommand = new MySqlCommand(commandText, connection);

            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            sqlCommand.Transaction = transaction;

            await sqlCommand.ExecuteNonQueryAsync().ConfigureAwait(false);

            if (!alreadyOpened)
                connection.Close();
        }


        public static async Task<SyncTable> GetTableDefinitionAsync(string tableName, MySqlConnection connection, MySqlTransaction transaction)
        {
            string commandColumn = "select * from information_schema.TABLES where table_schema = schema() and table_name = @tableName limit 1;";

            var tableNameParser = ParserName.Parse(tableName, "`");
            var syncTable = new SyncTable(tableNameParser.Unquoted().ToString());
            using var sqlCommand = new MySqlCommand(commandColumn, connection);
            sqlCommand.Parameters.AddWithValue("@tableName", tableNameParser.Unquoted().ToString());

            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            sqlCommand.Transaction = transaction;

            using (var reader = await sqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
                syncTable.Load(reader);

            if (!alreadyOpened)
                connection.Close();

            return syncTable;
        }

        public static async Task<SyncTable> GetColumnsForTableAsync(string tableName, MySqlConnection connection, MySqlTransaction transaction)
        {
            string commandColumn = "select * from information_schema.COLUMNS where table_schema = schema() and table_name = @tableName";

            var tableNameParser = ParserName.Parse(tableName, "`");
            var syncTable = new SyncTable(tableNameParser.Unquoted().ToString());
            using (var sqlCommand = new MySqlCommand(commandColumn, connection))
            {
                sqlCommand.Parameters.AddWithValue("@tableName", tableNameParser.Unquoted().ToString());

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

        public static async Task<SyncTable> GetPrimaryKeysForTableAsync(MySqlConnection connection, MySqlTransaction transaction, string tableName)
        {
            var commandColumn = @"select * from information_schema.COLUMNS where table_schema = schema() and table_name = @tableName and column_key='PRI'";

            var tableNameParser = ParserName.Parse(tableName, "`");
            var syncTable = new SyncTable(tableNameParser.Unquoted().ToString());
            using (var sqlCommand = new MySqlCommand(commandColumn, connection))
            {
                sqlCommand.Parameters.AddWithValue("@tableName", tableNameParser.Unquoted().ToString());

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

        public static async Task<SyncTable> GetRelationsForTableAsync(MySqlConnection connection, MySqlTransaction transaction, string tableName)
        {
            var commandRelations = @"
            SELECT
              ke.CONSTRAINT_NAME as ForeignKey,
              ke.POSITION_IN_UNIQUE_CONSTRAINT as ForeignKeyOrder,
              ke.referenced_table_name as ReferenceTableName,
              ke.REFERENCED_COLUMN_NAME as ReferenceColumnName,
              ke.table_name TableName,
              ke.COLUMN_NAME ColumnName
            FROM
              information_schema.KEY_COLUMN_USAGE ke
            WHERE
              ke.referenced_table_name IS NOT NULL
              and ke.table_schema = schema()
              AND ke.table_name = @tableName
            ORDER BY
              ke.referenced_table_name;";

            var tableNameParser = ParserName.Parse(tableName, "`");

            var syncTable = new SyncTable(tableNameParser.Unquoted().ToString());
            using (var sqlCommand = new MySqlCommand(commandRelations, connection))
            {
                sqlCommand.Parameters.AddWithValue("@tableName", tableNameParser.Unquoted().ToString());

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

        public static async Task DropTableIfExistsAsync(string tableName, MySqlConnection connection, MySqlTransaction transaction)
        {
            var tableNameParser = ParserName.Parse(tableName, "`");

            using var dbCommand = connection.CreateCommand();
            dbCommand.CommandText = $"select * from information_schema.TABLES where table_schema = schema() and table_name = @tableName";

            dbCommand.Parameters.AddWithValue("@tableName", tableNameParser.Unquoted().ToString());

            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            dbCommand.Transaction = transaction;

            await dbCommand.ExecuteNonQueryAsync().ConfigureAwait(false);

            if (!alreadyOpened)
                connection.Close();
        }

        public static async Task DropTriggerIfExistsAsync(MySqlConnection connection, MySqlTransaction transaction, string quotedTriggerName)
        {
            var triggerName = ParserName.Parse(quotedTriggerName, "`");

            using DbCommand dbCommand = connection.CreateCommand();
            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            dbCommand.CommandText = $"drop trigger {triggerName.Unquoted().ToString()}";
            dbCommand.Transaction = transaction;

            await dbCommand.ExecuteNonQueryAsync().ConfigureAwait(false);

            if (!alreadyOpened)
                connection.Close();
        }

        public static async Task<bool> TableExistsAsync(string tableName, MySqlConnection connection, MySqlTransaction transaction)
        {
            bool tableExist;
            var tableNameParser = ParserName.Parse(tableName, "`");

            using DbCommand dbCommand = connection.CreateCommand();

            dbCommand.CommandText = "select COUNT(*) from information_schema.TABLES where TABLE_NAME = @tableName and TABLE_SCHEMA = schema() and TABLE_TYPE = 'BASE TABLE'";

            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            dbCommand.Transaction = transaction;

            var sqlParameter = new MySqlParameter()
            {
                ParameterName = "@tableName",
                Value = tableNameParser.Unquoted().ToString()
            };

            dbCommand.Parameters.Add(sqlParameter);

            tableExist = ((Int64)await dbCommand.ExecuteScalarAsync().ConfigureAwait(false)) != 0;


            if (!alreadyOpened)
                connection.Close();

            return tableExist;
        }

        public static async Task<bool> TriggerExistsAsync(MySqlConnection connection, MySqlTransaction transaction, string quotedTriggerName)
        {
            bool triggerExist;
            var triggerName = ParserName.Parse(quotedTriggerName, "`");

            using (var dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "select count(*) from information_schema.TRIGGERS where trigger_name = @triggerName AND trigger_schema = schema()";

                dbCommand.Parameters.AddWithValue("@triggerName", triggerName.Unquoted().ToString());

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                dbCommand.Transaction = transaction;

                triggerExist = ((long)await dbCommand.ExecuteScalarAsync().ConfigureAwait(false)) != 0L;


                if (!alreadyOpened)
                    connection.Close();

            }
            return triggerExist;
        }

        public static async Task<bool> ProcedureExistsAsync(MySqlConnection connection, MySqlTransaction transaction, string commandName)
        {
            bool procExist;
            var commandNameString = ParserName.Parse(commandName, "`");

            using (var dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = @"select count(*) from information_schema.ROUTINES
                                        where ROUTINE_TYPE = 'PROCEDURE'
                                        and ROUTINE_SCHEMA = schema()
                                        and ROUTINE_NAME = @procName";

                dbCommand.Parameters.AddWithValue("@procName", commandNameString.Unquoted().ToString());

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                dbCommand.Transaction = transaction;

                procExist = ((long)await dbCommand.ExecuteScalarAsync().ConfigureAwait(false)) != 0L;


                if (!alreadyOpened)
                    connection.Close();

            }
            return procExist;
        }

        public static string JoinTwoTablesOnClause(IEnumerable<SyncColumn> pkeys, string leftName, string rightName)
        {
            var stringBuilder = new StringBuilder();
            string strRightName = (string.IsNullOrEmpty(rightName) ? string.Empty : string.Concat(rightName, "."));
            string strLeftName = (string.IsNullOrEmpty(leftName) ? string.Empty : string.Concat(leftName, "."));

            string str = "";
            foreach (var column in pkeys)
            {
                var quotedColumn = ParserName.Parse(column, "`");

                stringBuilder.Append(str);
                stringBuilder.Append(strLeftName);
                stringBuilder.Append(quotedColumn.Quoted().ToString());
                stringBuilder.Append(" = ");
                stringBuilder.Append(strRightName);
                stringBuilder.Append(quotedColumn.Quoted().ToString());

                str = " AND ";
            }
            return stringBuilder.ToString();
        }

        public static string ColumnsAndParameters(IEnumerable<SyncColumn> pkeys, string fromPrefix)
        {
            var prefix_parameter = MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER;
            var stringBuilder = new StringBuilder();
            string strFromPrefix = (string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, "."));
            string str1 = "";
            foreach (var pkey in pkeys)
            {
                var quotedColumn = ParserName.Parse(pkey, "`");

                stringBuilder.Append(str1);
                stringBuilder.Append(strFromPrefix);
                stringBuilder.Append(quotedColumn.Quoted().ToString());
                stringBuilder.Append(" = ");
                stringBuilder.Append(prefix_parameter).Append(quotedColumn.Unquoted().Normalized().ToString());
                str1 = " AND ";
            }
            return stringBuilder.ToString();
        }

        public static string WhereColumnAndParameters(IEnumerable<SyncColumn> primaryKeys, string fromPrefix, string mysql_prefix = MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER)
        {
            StringBuilder stringBuilder = new StringBuilder();
            string strFromPrefix = string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, ".");
            string str1 = "";
            foreach (var column in primaryKeys)
            {
                var quotedColumn = ParserName.Parse(column, "`");
                var paramQuotedColumn = ParserName.Parse($"{mysql_prefix}{column.ColumnName}", "`");

                stringBuilder.Append(str1);
                stringBuilder.Append(strFromPrefix);
                stringBuilder.Append(quotedColumn.Quoted().ToString());
                stringBuilder.Append(" = ");
                stringBuilder.Append(paramQuotedColumn.Quoted());
                str1 = " AND ";
            }
            return stringBuilder.ToString();
        }

        public static string CommaSeparatedUpdateFromParameters(SyncTable table, string fromPrefix = "", string mysql_prefix = MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER)
        {
            var stringBuilder = new StringBuilder();
            string strFromPrefix = (string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, "."));
            string strSeparator = "";
            foreach (var mutableColumn in table.GetMutableColumns())
            {
                var quotedColumn = ParserName.Parse(mutableColumn.ColumnName, "`");
                var argQuotedColumn = ParserName.Parse($"{mysql_prefix}{mutableColumn.ColumnName}", "`");
                stringBuilder.Append(strSeparator).Append(' ').Append(strFromPrefix).Append(quotedColumn.Quoted().ToString()).Append(" = ").AppendLine(argQuotedColumn.Quoted().Normalized().ToString());
                strSeparator = ", ";
            }
            return stringBuilder.ToString();

        }
    }
}
