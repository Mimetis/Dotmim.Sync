using Dotmim.Sync.Builders;
#if MARIADB
using Dotmim.Sync.MariaDB.Builders;
#elif MYSQL
using Dotmim.Sync.MySql.Builders;
#endif

#if NET5_0 || NET6_0 || NETCOREAPP3_1
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
#if MARIADB
    public static class MariaDBManagementUtils
#elif MYSQL
    public static class MySqlManagementUtils
#endif
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


        public static async Task<SyncTable> GetTableAsync(MySqlConnection connection, MySqlTransaction transaction, string tableName)
        {
            string commandColumn = "select * from information_schema.TABLES where table_schema = schema() and table_name = @tableName limit 1;";

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

        public static async Task<SyncTable> GetColumnsForTableAsync(MySqlConnection connection, MySqlTransaction transaction, string tableName)
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

        public static async Task DropTableIfExistsAsync(MySqlConnection connection, MySqlTransaction transaction, string quotedTableName)
        {
            var tableNameParser = ParserName.Parse(quotedTableName, "`");

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

        public static async Task<bool> TableExistsAsync(MySqlConnection connection, MySqlTransaction transaction, ParserName table)
        {
            bool tableExist;


            using (DbCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "select COUNT(*) from information_schema.TABLES where TABLE_NAME = @tableName and TABLE_SCHEMA = schema() and TABLE_TYPE = 'BASE TABLE'";

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                dbCommand.Transaction = transaction;

                var sqlParameter = new MySqlParameter()
                {
                    ParameterName = "@tableName",
                    Value = table.Unquoted().ToString()
                };

                dbCommand.Parameters.Add(sqlParameter);

                tableExist = ((Int64)await dbCommand.ExecuteScalarAsync().ConfigureAwait(false)) != 0;


                if (!alreadyOpened)
                    connection.Close();


            }

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

#if MARIADB
            var prefix_parameter = MariaDBBuilderProcedure.MYSQL_PREFIX_PARAMETER;
#elif MYSQL
            var prefix_parameter = MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER;
#endif
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
                stringBuilder.Append($"{prefix_parameter}{quotedColumn.Unquoted().Normalized().ToString()}");
                str1 = " AND ";
            }
            return stringBuilder.ToString();
        }

#if MARIADB
        public static string WhereColumnAndParameters(IEnumerable<SyncColumn> primaryKeys, string fromPrefix, string mysql_prefix = MariaDBBuilderProcedure.MYSQL_PREFIX_PARAMETER)
#elif MYSQL
        public static string WhereColumnAndParameters(IEnumerable<SyncColumn> primaryKeys, string fromPrefix, string mysql_prefix = MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER)
#endif
        {
            StringBuilder stringBuilder = new StringBuilder();
            string strFromPrefix = (string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, "."));
            string str1 = "";
            foreach (var column in primaryKeys)
            {
                var quotedColumn = ParserName.Parse(column, "`");

                stringBuilder.Append(str1);
                stringBuilder.Append(strFromPrefix);
                stringBuilder.Append(quotedColumn.Quoted().ToString());
                stringBuilder.Append(" = ");
                stringBuilder.Append($"{mysql_prefix}{quotedColumn.Unquoted().Normalized().ToString()}");
                str1 = " AND ";
            }
            return stringBuilder.ToString();
        }

#if MARIADB
        public static string CommaSeparatedUpdateFromParameters(SyncTable table, string fromPrefix = "", string mysql_prefix = MariaDBBuilderProcedure.MYSQL_PREFIX_PARAMETER)
#elif MYSQL
        public static string CommaSeparatedUpdateFromParameters(SyncTable table, string fromPrefix = "", string mysql_prefix = MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER)
#endif
        {
            var stringBuilder = new StringBuilder();
            string strFromPrefix = (string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, "."));
            string strSeparator = "";
            foreach (var mutableColumn in table.GetMutableColumns())
            {
                var quotedColumn = ParserName.Parse(mutableColumn, "`");
                stringBuilder.AppendLine($"{strSeparator} {strFromPrefix}{quotedColumn.Quoted().ToString()} = {mysql_prefix}{quotedColumn.Unquoted().Normalized().ToString()}");
                strSeparator = ", ";
            }
            return stringBuilder.ToString();

        }
    }
}
