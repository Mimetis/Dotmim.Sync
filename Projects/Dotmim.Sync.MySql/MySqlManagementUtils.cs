using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Text.RegularExpressions;

namespace Dotmim.Sync.MySql
{
    public static class MySqlManagementUtils
    {

        public static DmTable ColumnsForTable(MySqlConnection connection, MySqlTransaction transaction, string tableName)
        {
            string commandColumn = "select * from information_schema.COLUMNS where table_schema = schema() and table_name = @tableName";

            var tableNameParser = ParserName.Parse(tableName, "`");
            DmTable dmTable = new DmTable(tableNameParser.Unquoted().ToString());
            using (MySqlCommand sqlCommand = new MySqlCommand(commandColumn, connection, transaction))
            {
                sqlCommand.Parameters.AddWithValue("@tableName", tableNameParser.Unquoted().ToString());

                using (var reader = sqlCommand.ExecuteReader())
                {
                    dmTable.Fill(reader);
                }
            }
            return dmTable;
        }

        internal static DmTable PrimaryKeysForTable(MySqlConnection connection, MySqlTransaction transaction, string tableName)
        {

            var commandColumn = @"select * from information_schema.COLUMNS where table_schema = schema() and table_name = @tableName and column_key='PRI'";

            var tableNameParser = ParserName.Parse(tableName, "`");
            var dmTable = new DmTable(tableNameParser.Unquoted().ToString());
            using (var sqlCommand = new MySqlCommand(commandColumn, connection, transaction))
            {
                sqlCommand.Parameters.AddWithValue("@tableName", tableNameParser.Unquoted().ToString());

                using (var reader = sqlCommand.ExecuteReader())
                {
                    dmTable.Fill(reader);
                }
            }
            return dmTable;
        }

        internal static DmTable RelationsForTable(MySqlConnection connection, MySqlTransaction transaction, string tableName)
        {
            var commandRelations = @"
            SELECT
              ke.CONSTRAINT_NAME as ForeignKey,
              ke.POSITION_IN_UNIQUE_CONSTRAINT as ForeignKeyOrder,
              ke.referenced_table_name as TableName,
              ke.REFERENCED_COLUMN_NAME as ColumnName,
              ke.table_name ReferenceTableName,
              ke.COLUMN_NAME ReferenceColumnName
            FROM
              information_schema.KEY_COLUMN_USAGE ke
            WHERE
              ke.referenced_table_name IS NOT NULL
              and ke.REFERENCED_TABLE_SCHEMA = schema()
              AND ke.REFERENCED_TABLE_NAME = @tableName
            ORDER BY
              ke.referenced_table_name;";

            var tableNameParser = ParserName.Parse(tableName, "`");

            var dmTable = new DmTable(tableNameParser.Unquoted().ToString());
            using (MySqlCommand sqlCommand = new MySqlCommand(commandRelations, connection, transaction))
            {
                sqlCommand.Parameters.AddWithValue("@tableName", tableNameParser.Unquoted().ToString());

                using (var reader = sqlCommand.ExecuteReader())
                {
                    dmTable.Fill(reader);
                }
            }


            return dmTable;

        }

        public static void DropTableIfExists(MySqlConnection connection, MySqlTransaction transaction, string quotedTableName)
        {
            var tableNameParser = ParserName.Parse(quotedTableName, "`");

            using (var dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = $"select * from information_schema.TABLES where table_schema = schema() and table_name = @tableName";

                dbCommand.Parameters.AddWithValue("@tableName", tableNameParser.Unquoted().ToString());

                if (transaction != null)
                    dbCommand.Transaction = transaction;

                dbCommand.ExecuteNonQuery();
            }
        }

        public static string DropTableIfExistsScriptText(string quotedTableName)
        {
            var tableNameParser = ParserName.Parse(quotedTableName, "`");

            return $"drop table if exist {tableNameParser.Unquoted().ToString()}";
        }

        internal static bool IsStringNullOrWhitespace(string value)
        {
            return Regex.Match(value ?? string.Empty, "^\\s*$").Success;
        }
        public static string DropTableScriptText(string quotedTableName)
        {
            var tableNameParser = ParserName.Parse(quotedTableName, "`");

            return $"drop table {tableNameParser.Unquoted().ToString()}";
        }

        public static void DropTriggerIfExists(MySqlConnection connection, MySqlTransaction transaction, string quotedTriggerName)
        {
            var triggerName = ParserName.Parse(quotedTriggerName, "`");

            using (DbCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = $"drop trigger {triggerName.Unquoted().ToString()}";
                if (transaction != null)
                    dbCommand.Transaction = transaction;

                dbCommand.ExecuteNonQuery();
            }
        }

        public static string DropTriggerScriptText(string quotedTriggerName)
        {
            var triggerName = ParserName.Parse(quotedTriggerName, "`");
            return $"drop trigger {triggerName.Unquoted().ToString()}";
        }

        public static bool TableExists(MySqlConnection connection, MySqlTransaction transaction, ParserName table)
        {
            bool tableExist;

            using (DbCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "select COUNT(*) from information_schema.TABLES where TABLE_NAME = @tableName and TABLE_SCHEMA = schema() and TABLE_TYPE = 'BASE TABLE'";

                if (transaction != null)
                    dbCommand.Transaction = transaction;

                var sqlParameter = new MySqlParameter()
                {
                    ParameterName = "@tableName",
                    Value = table.Unquoted().ToString()
                };

                dbCommand.Parameters.Add(sqlParameter);

                tableExist = (Int64)dbCommand.ExecuteScalar() != 0;

            }
            return tableExist;
        }

        public static bool TriggerExists(MySqlConnection connection, MySqlTransaction transaction, string quotedTriggerName)
        {
            bool triggerExist;
            var triggerName = ParserName.Parse(quotedTriggerName, "`");


            using (var dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "select count(*) from information_schema.TRIGGERS where trigger_name = @triggerName AND trigger_schema = schema()";

                dbCommand.Parameters.AddWithValue("@triggerName", triggerName.Unquoted().ToString());

                if (transaction != null)
                    dbCommand.Transaction = transaction;

                triggerExist = (long)dbCommand.ExecuteScalar() != 0L;
            }
            return triggerExist;
        }

        internal static bool ProcedureExists(MySqlConnection connection, MySqlTransaction transaction, string commandName)
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

                if (transaction != null)
                    dbCommand.Transaction = transaction;

                procExist = (long)dbCommand.ExecuteScalar() != 0L;
            }
            return procExist;
        }

        internal static string JoinTwoTablesOnClause(IEnumerable<string> pkeys, string leftName, string rightName)
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

        internal static string ColumnsAndParameters(IEnumerable<string> pkeys, string fromPrefix)
        {
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
                stringBuilder.Append($"{MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER}{pkey}");
                str1 = " AND ";
            }
            return stringBuilder.ToString();
        }

        internal static string WhereColumnAndParameters(IEnumerable<string> primaryKeys, string fromPrefix)
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
                stringBuilder.Append($"{MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER}{column}");
                str1 = " AND ";
            }
            return stringBuilder.ToString();
        }

        internal static string CommaSeparatedUpdateFromParameters(SyncTable table, string fromPrefix = "")
        {
            var stringBuilder = new StringBuilder();
            string strFromPrefix = (string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, "."));
            string strSeparator = "";
            foreach (var mutableColumn in table.GetMutableColumns())
            {
                var quotedColumn = ParserName.Parse(mutableColumn, "`");
                stringBuilder.AppendLine($"{strSeparator} {strFromPrefix}{quotedColumn.Quoted().ToString()} = {MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER}{quotedColumn.Unquoted().Normalized().ToString()}");
                strSeparator = ", ";
            }
            return stringBuilder.ToString();

        }
    }
}
