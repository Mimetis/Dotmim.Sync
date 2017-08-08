using Dotmim.Sync.Data;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Linq;
using System.Data.SQLite;
using Dotmim.Sync.Builders;

namespace Dotmim.Sync.SQLite
{
    internal static class SQLiteManagementUtils
    {

        public static void DropTableIfExists(SQLiteConnection connection, SQLiteTransaction transaction, string quotedTableName)
        {
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTableName);

            using (DbCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = $"drop table if exist {objectNameParser.ObjectName}";
                if (transaction != null)
                    dbCommand.Transaction = transaction;

                dbCommand.ExecuteNonQuery();
            }
        }

        public static string DropTableIfExistsScriptText(string quotedTableName)
        {
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTableName);

            return $"drop table if exist {objectNameParser.ObjectName}";
        }

        public static string DropTableScriptText(string quotedTableName)
        {
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTableName);

            return $"drop table {objectNameParser.ObjectName}";
        }

        public static void DropTriggerIfExists(SQLiteConnection connection, SQLiteTransaction transaction, string quotedTriggerName)
        {
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTriggerName);

            using (DbCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = $"drop trigger if exist {objectNameParser.ObjectName}";
                if (transaction != null)
                    dbCommand.Transaction = transaction;
             
                dbCommand.ExecuteNonQuery();
            }
        }

        public static string DropTriggerScriptText(string quotedTriggerName)
        {
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTriggerName);
            return $"drop trigger {objectNameParser.ObjectName}";
        }


        public static bool TableExists(SQLiteConnection connection, SQLiteTransaction transaction, string quotedTableName)
        {
            bool tableExist;
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTableName);
            using (DbCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "select count(*) from sqlite_master where name = @tableName AND type='table'";

                SQLiteParameter SQLiteParameter = new SQLiteParameter()
                {
                    ParameterName = "@tableName",
                    Value = objectNameParser.ObjectName
                };
                dbCommand.Parameters.Add(SQLiteParameter);

                if (transaction != null)
                    dbCommand.Transaction = transaction;

                tableExist = (long)dbCommand.ExecuteScalar() != 0L;
            }
            return tableExist;
        }

        public static bool TriggerExists(SQLiteConnection connection, SQLiteTransaction transaction, string quotedTriggerName)
        {
            bool triggerExist;
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTriggerName);


            using (SQLiteCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "select count(*) from sqlite_master where name = @triggerName AND type='trigger'";

                dbCommand.Parameters.AddWithValue("@triggerName", objectNameParser.ObjectName);

                if (transaction != null)
                    dbCommand.Transaction = transaction;

                triggerExist = (long)dbCommand.ExecuteScalar() != 0L;
            }
            return triggerExist;
        }

        internal static string JoinTwoTablesOnClause(IEnumerable<DmColumn> columns, string leftName, string rightName)
        {
            StringBuilder stringBuilder = new StringBuilder();
            string strRightName = (string.IsNullOrEmpty(rightName) ? string.Empty : string.Concat(rightName, "."));
            string strLeftName = (string.IsNullOrEmpty(leftName) ? string.Empty : string.Concat(leftName, "."));

            string str = "";
            foreach (DmColumn column in columns)
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName);

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

        internal static string WhereColumnAndParameters(IEnumerable<DmColumn> columns, string fromPrefix)
        {
            StringBuilder stringBuilder = new StringBuilder();
            string strFromPrefix = (string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, "."));
            string str1 = "";
            foreach (DmColumn column in columns)
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName);

                stringBuilder.Append(str1);
                stringBuilder.Append(strFromPrefix);
                stringBuilder.Append(quotedColumn.QuotedString);
                stringBuilder.Append(" = ");
                stringBuilder.Append($"@{column.ColumnName}");
                str1 = " AND ";
            }
            return stringBuilder.ToString();
        }

        internal static string CommaSeparatedUpdateFromParameters(DmTable table, string fromPrefix = "")
        {
            StringBuilder stringBuilder = new StringBuilder();
            string strFromPrefix = (string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, "."));
            string strSeparator = "";
            foreach (DmColumn column in table.NonPkColumns.Where(c => !c.ReadOnly))
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName);
                stringBuilder.AppendLine($"{strSeparator} {strFromPrefix}{quotedColumn.QuotedString} = @{quotedColumn.UnquotedString}");
                strSeparator = ", ";
            }
            return stringBuilder.ToString();

        }
    }
}
