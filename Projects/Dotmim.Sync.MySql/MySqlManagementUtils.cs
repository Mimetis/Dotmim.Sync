using Dotmim.Sync.Data;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Linq;
using MySql.Data.MySqlClient;
using Dotmim.Sync.Builders;
using System;
using System.Text.RegularExpressions;

namespace Dotmim.Sync.MySql
{
    public static class MySqlManagementUtils
    {

        public static DmTable ColumnsForTable(MySqlConnection connection, MySqlTransaction transaction, string tableName)
        {
            string commandColumn = "select * from information_schema.COLUMNS where table_schema = schema() and table_name = @tableName";

            ObjectNameParser tableNameParser = new ObjectNameParser(tableName, "`", "`");
            DmTable dmTable = new DmTable(tableNameParser.UnquotedStringWithUnderScore);
            using (MySqlCommand sqlCommand = new MySqlCommand(commandColumn, connection, transaction))
            {
                sqlCommand.Parameters.AddWithValue("@tableName", tableNameParser.ObjectName);

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

            ObjectNameParser tableNameParser = new ObjectNameParser(tableName, "`", "`");
            DmTable dmTable = new DmTable(tableNameParser.UnquotedStringWithUnderScore);
            using (MySqlCommand sqlCommand = new MySqlCommand(commandColumn, connection, transaction))
            {
                sqlCommand.Parameters.AddWithValue("@tableName", tableNameParser.ObjectName);

                using (var reader = sqlCommand.ExecuteReader())
                {
                    dmTable.Fill(reader);
                }
            }
            return dmTable;
        }

        internal static DmTable RelationsForTable(MySqlConnection connection, MySqlTransaction transaction, string tableName)
        {
            var commandRelations = @"Select CONSTRAINT_NAME as ForeignKey,
		                                    TABLE_NAME as TableName,
                                            COLUMN_NAME as ColumnName,
                                            REFERENCED_TABLE_NAME as ReferenceTableName,
                                            REFERENCED_COLUMN_NAME as ReferenceColumnName
                                    from INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                                    Where TABLE_SCHEMA = schema() 
                                    and REFERENCED_TABLE_NAME is not null and TABLE_NAME = @tableName";

            ObjectNameParser tableNameParser = new ObjectNameParser(tableName, "`", "`");
            DmTable dmTable = new DmTable(tableNameParser.UnquotedStringWithUnderScore);
            using (MySqlCommand sqlCommand = new MySqlCommand(commandRelations, connection, transaction))
            {
                sqlCommand.Parameters.AddWithValue("@tableName", tableNameParser.ObjectName);

                using (var reader = sqlCommand.ExecuteReader())
                {
                    dmTable.Fill(reader);
                }
            }


            return dmTable;

        }

        public static void DropTableIfExists(MySqlConnection connection, MySqlTransaction transaction, string quotedTableName)
        {
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTableName, "`", "`");

            using (MySqlCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = $"select * from information_schema.TABLES where table_schema = schema() and table_name = @tableName";

                dbCommand.Parameters.AddWithValue("@tableName", objectNameParser.ObjectName);
               
                if (transaction != null)
                    dbCommand.Transaction = transaction;

                dbCommand.ExecuteNonQuery();
            }
        }

        public static string DropTableIfExistsScriptText(string quotedTableName)
        {
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTableName, "`", "`");

            return $"drop table if exist {objectNameParser.ObjectName}";
        }

        internal static bool IsStringNullOrWhitespace(string value)
        {
            return Regex.Match(value ?? string.Empty, "^\\s*$").Success;
        }
        public static string DropTableScriptText(string quotedTableName)
        {
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTableName, "`", "`");

            return $"drop table {objectNameParser.ObjectName}";
        }

        public static void DropTriggerIfExists(MySqlConnection connection, MySqlTransaction transaction, string quotedTriggerName)
        {
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTriggerName, "`", "`");

            using (DbCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = $"drop trigger {objectNameParser.ObjectName}";
                if (transaction != null)
                    dbCommand.Transaction = transaction;
             
                dbCommand.ExecuteNonQuery();
            }
        }

        public static string DropTriggerScriptText(string quotedTriggerName)
        {
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTriggerName, "`", "`");
            return $"drop trigger {objectNameParser.ObjectName}";
        }

        public static bool TableExists(MySqlConnection connection, MySqlTransaction transaction, string unQuotedTableName)
        {
            bool tableExist;
            ObjectNameParser tableNameParser = new ObjectNameParser(unQuotedTableName, "`", "`");
            using (DbCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "select COUNT(*) from information_schema.TABLES where TABLE_NAME = @tableName and TABLE_SCHEMA = schema() and TABLE_TYPE = 'BASE TABLE'";

                if (transaction != null)
                    dbCommand.Transaction = transaction;

                MySqlParameter sqlParameter = new MySqlParameter()
                {
                    ParameterName = "@tableName",
                    Value = tableNameParser.UnquotedString
                };

                dbCommand.Parameters.Add(sqlParameter);

                tableExist = (Int64)dbCommand.ExecuteScalar() != 0;

            }
            return tableExist;
        }

        public static bool TriggerExists(MySqlConnection connection, MySqlTransaction transaction, string quotedTriggerName)
        {
            bool triggerExist;
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTriggerName, "`", "`");


            using (MySqlCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "select count(*) from information_schema.TRIGGERS where trigger_name = @triggerName AND trigger_schema = schema()";

                dbCommand.Parameters.AddWithValue("@triggerName", objectNameParser.ObjectName);

                if (transaction != null)
                    dbCommand.Transaction = transaction;

                triggerExist = (long)dbCommand.ExecuteScalar() != 0L;
            }
            return triggerExist;
        }

        internal static bool ProcedureExists(MySqlConnection connection, MySqlTransaction transaction, string commandName)
        {
            bool procExist;
            ObjectNameParser objectNameParser = new ObjectNameParser(commandName, "`", "`");


            using (MySqlCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = @"select count(*) from information_schema.ROUTINES
                                        where ROUTINE_TYPE = 'PROCEDURE'
                                        and ROUTINE_SCHEMA = schema()
                                        and ROUTINE_NAME = @procName";

                dbCommand.Parameters.AddWithValue("@procName", objectNameParser.ObjectName);

                if (transaction != null)
                    dbCommand.Transaction = transaction;

                procExist = (long)dbCommand.ExecuteScalar() != 0L;
            }
            return procExist;
        }

        internal static string JoinTwoTablesOnClause(IEnumerable<DmColumn> columns, string leftName, string rightName)
        {
            StringBuilder stringBuilder = new StringBuilder();
            string strRightName = (string.IsNullOrEmpty(rightName) ? string.Empty : string.Concat(rightName, "."));
            string strLeftName = (string.IsNullOrEmpty(leftName) ? string.Empty : string.Concat(leftName, "."));

            string str = "";
            foreach (DmColumn column in columns)
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName, "`", "`");

                stringBuilder.Append(str);
                stringBuilder.Append(strLeftName);
                stringBuilder.Append(quotedColumn.QuotedString);
                stringBuilder.Append(" = ");
                stringBuilder.Append(strRightName);
                stringBuilder.Append(quotedColumn.QuotedString);

                str = " AND ";
            }
            return stringBuilder.ToString();
        }

        internal static string ColumnsAndParameters(IEnumerable<DmColumn> columns, string fromPrefix)
        {
            StringBuilder stringBuilder = new StringBuilder();
            string strFromPrefix = (string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, "."));
            string str1 = "";
            foreach (DmColumn column in columns)
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName, "`", "`");

                stringBuilder.Append(str1);
                stringBuilder.Append(strFromPrefix);
                stringBuilder.Append(quotedColumn.QuotedString);
                stringBuilder.Append(" = ");
                stringBuilder.Append($"{MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER}{column.ColumnName}");
                str1 = " AND ";
            }
            return stringBuilder.ToString();
        }

        internal static string WhereColumnAndParameters(IEnumerable<DmColumn> columns, string fromPrefix)
        {
            StringBuilder stringBuilder = new StringBuilder();
            string strFromPrefix = (string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, "."));
            string str1 = "";
            foreach (DmColumn column in columns)
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName,"`", "`");

                stringBuilder.Append(str1);
                stringBuilder.Append(strFromPrefix);
                stringBuilder.Append(quotedColumn.QuotedString);
                stringBuilder.Append(" = ");
                stringBuilder.Append($"{MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER}{column.ColumnName}");
                str1 = " AND ";
            }
            return stringBuilder.ToString();
        }

        internal static string CommaSeparatedUpdateFromParameters(DmTable table, string fromPrefix = "")
        {
            StringBuilder stringBuilder = new StringBuilder();
            string strFromPrefix = (string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, "."));
            string strSeparator = "";
            foreach (DmColumn mutableColumn in table.MutableColumns)
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(mutableColumn.ColumnName, "`", "`");
                stringBuilder.AppendLine($"{strSeparator} {strFromPrefix}{quotedColumn.QuotedString} = {MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER}{quotedColumn.UnquotedString}");
                strSeparator = ", ";
            }
            return stringBuilder.ToString();

        }
    }
}
