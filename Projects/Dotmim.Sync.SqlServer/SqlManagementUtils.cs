using Dotmim.Sync.Core.Common;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace Dotmim.Sync.SqlServer
{
    internal static class SqlManagementUtils
    {

        /// <summary>
        /// Get columns for table
        /// </summary>
        public static List<string> ColumnsForTable(SqlConnection connection, SqlTransaction transaction, string quotedString)
        {
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedString);
            List<string> strs = new List<string>();
            using (SqlCommand sqlCommand = new SqlCommand("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @tableName AND TABLE_SCHEMA = @schemaName", connection, transaction))
            {
                sqlCommand.Parameters.AddWithValue("@tableName", objectNameParser.ObjectName);
                sqlCommand.Parameters.AddWithValue("@schemaName", SqlManagementUtils.GetUnquotedSqlSchemaName(objectNameParser));
                using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader())
                {
                    while (sqlDataReader.Read())
                    {
                        strs.Add(string.Concat("[", sqlDataReader.GetString(0), "]"));
                    }
                }
            }
            return strs;
        }

        /// <summary>
        /// Drop a stored proc if exists
        /// </summary>
        public static void DropProcedureIfExists(SqlConnection connection, SqlTransaction transaction, string quotedProcedureName)
        {
            SqlManagementUtils.DropProcedureIfExists(connection, transaction, 30, quotedProcedureName);
        }

        public static void DropProcedureIfExists(SqlConnection connection, SqlTransaction transaction, int commandTimout, string quotedProcedureName)
        {
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedProcedureName);
            using (SqlCommand sqlCommand = new SqlCommand(string.Format(CultureInfo.InvariantCulture, "IF EXISTS (SELECT * FROM sys.procedures p JOIN sys.schemas s ON s.schema_id = p.schema_id WHERE p.name = @procName AND s.name = @schemaName) DROP PROCEDURE {0}", quotedProcedureName), connection, transaction))
            {
                sqlCommand.CommandTimeout = commandTimout;
                sqlCommand.Parameters.AddWithValue("@procName", objectNameParser.ObjectName);
                sqlCommand.Parameters.AddWithValue("@schemaName", SqlManagementUtils.GetUnquotedSqlSchemaName(objectNameParser));
                sqlCommand.ExecuteNonQuery();
            }
        }

        public static string DropProcedureScriptText(string quotedProcedureName)
        {
            return string.Format(CultureInfo.InvariantCulture, "DROP PROCEDURE {0};\n", quotedProcedureName);
        }

        public static void DropTableIfExists(SqlConnection connection, SqlTransaction transaction, string quotedTableName)
        {
            SqlManagementUtils.DropTableIfExists(connection, transaction, 30, quotedTableName);
        }

        public static void DropTableIfExists(SqlConnection connection, SqlTransaction transaction, int commandTimeout, string quotedTableName)
        {
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTableName);
            using (SqlCommand sqlCommand = new SqlCommand(string.Format(CultureInfo.InvariantCulture, "IF EXISTS (SELECT t.name FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.name = @tableName AND s.name = @schemaName) DROP TABLE {0}", quotedTableName), connection, transaction))
            {
                sqlCommand.CommandTimeout = commandTimeout;
                sqlCommand.Parameters.AddWithValue("@tableName", objectNameParser.ObjectName);
                sqlCommand.Parameters.AddWithValue("@schemaName", SqlManagementUtils.GetUnquotedSqlSchemaName(objectNameParser));
                sqlCommand.ExecuteNonQuery();
            }
        }

        public static string DropTableIfExistsScriptText(string quotedTableName)
        {
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTableName);
            object[] escapedString = new object[] { objectNameParser.ObjectName, SqlManagementUtils.GetUnquotedSqlSchemaName(objectNameParser), quotedTableName };
            return string.Format(CultureInfo.InvariantCulture, "IF EXISTS (SELECT t.name FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.name = N'{0}' AND s.name = N'{1}') DROP TABLE {2}\n", escapedString);
        }

        public static string DropTableScriptText(string quotedTableName)
        {
            CultureInfo invariantCulture = CultureInfo.InvariantCulture;
            object[] objArray = new object[] { quotedTableName };
            return string.Format(invariantCulture, "DROP TABLE {0};\n", objArray);
        }

        public static void DropTriggerIfExists(SqlConnection connection, SqlTransaction transaction, string quotedTriggerName)
        {
            SqlManagementUtils.DropTriggerIfExists(connection, transaction, 30, quotedTriggerName);
        }

        public static void DropTriggerIfExists(SqlConnection connection, SqlTransaction transaction, int commandTimeout, string quotedTriggerName)
        {
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTriggerName);
            using (SqlCommand sqlCommand = new SqlCommand(string.Format(CultureInfo.InvariantCulture, "IF EXISTS (SELECT tr.name FROM sys.triggers tr JOIN sys.tables t ON tr.parent_id = t.object_id JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE tr.name = @triggerName and s.name = @schemaName) DROP TRIGGER {0}", quotedTriggerName), connection, transaction))
            {
                sqlCommand.CommandTimeout = commandTimeout;
                sqlCommand.Parameters.AddWithValue("@triggerName", objectNameParser.ObjectName);
                sqlCommand.Parameters.AddWithValue("@schemaName", SqlManagementUtils.GetUnquotedSqlSchemaName(objectNameParser));
                sqlCommand.ExecuteNonQuery();
            }
        }

        public static string DropTriggerScriptText(string quotedTriggerName)
        {
            return string.Format(CultureInfo.InvariantCulture, "DROP TRIGGER {0};\n", quotedTriggerName);
        }

        public static void DropTypeIfExists(SqlConnection connection, SqlTransaction transaction, string quotedTypeName)
        {
            SqlManagementUtils.DropTypeIfExists(connection, transaction, 30, quotedTypeName);
        }

        public static void DropTypeIfExists(SqlConnection connection, SqlTransaction transaction, int commandTimeout, string quotedTypeName)
        {
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTypeName);
            using (SqlCommand sqlCommand = new SqlCommand(string.Format(CultureInfo.InvariantCulture, "IF EXISTS (SELECT * FROM sys.types t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.name = @typeName AND s.name = @schemaName) DROP TYPE {0}", quotedTypeName), connection, transaction))
            {
                sqlCommand.CommandTimeout = commandTimeout;
                sqlCommand.Parameters.AddWithValue("@typeName", objectNameParser.ObjectName);
                sqlCommand.Parameters.AddWithValue("@schemaName", SqlManagementUtils.GetUnquotedSqlSchemaName(objectNameParser));
                sqlCommand.ExecuteNonQuery();
            }
        }

        public static string DropTypeScriptText(string quotedTypeName)
        {
            return string.Format(CultureInfo.InvariantCulture, "DROP TYPE {0};\n", quotedTypeName);
        }

        //public static List<string> GetAllScopeNames(string objectPrefix, string objectSchema, SqlConnection connection, string scopeNameCol)
        //{
        //    List<string> strs = new List<string>();
        //    string quotedPrefixedName = ManagementUtils.GetQuotedPrefixedName(objectPrefix, ManagementUtils.ScopeTableDefaultName, objectSchema);
        //    SqlCommand sqlCommand = new SqlCommand(string.Concat("SELECT [", scopeNameCol, "] FROM ", quotedPrefixedName), connection);
        //    SqlDataReader sqlDataReader = null;
        //    try
        //    {
        //        sqlDataReader = sqlCommand.ExecuteReader();
        //        int ordinal = sqlDataReader.GetOrdinal(scopeNameCol);
        //        while (sqlDataReader.Read())
        //        {
        //            strs.Add(sqlDataReader.GetString(ordinal));
        //        }
        //    }
        //    finally
        //    {
        //        if (sqlCommand != null)
        //        {
        //            sqlCommand.Dispose();
        //        }
        //        if (sqlDataReader != null)
        //        {

        //            sqlDataReader.Dispose();
        //        }
        //    }
        //    return strs;
        //}

        internal static string GetObjectSchemaValue(string value)
        {
            string empty = value ?? string.Empty;
            if (empty.Equals("dbo", StringComparison.OrdinalIgnoreCase))
                empty = string.Empty;

            return empty;
        }

   
        public static string GetUnquotedSqlSchemaName(ObjectNameParser parser)
        {
            if (string.IsNullOrEmpty(parser.SchemaName))
                return "dbo";

            return parser.SchemaName;
        }

        internal static bool IsStringNullOrWhitespace(string value)
        {
            return Regex.Match(value ?? string.Empty, "^\\s*$").Success;
        }

        public static bool ProcedureExists(SqlConnection connection, SqlTransaction transaction, string quotedProcedureName)
        {
            bool flag;
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedProcedureName);
            using (SqlCommand sqlCommand = new SqlCommand("IF EXISTS (SELECT * FROM sys.procedures p JOIN sys.schemas s ON s.schema_id = p.schema_id WHERE p.name = @procName AND s.name = @schemaName) SELECT 1 ELSE SELECT 0", connection))
            {
                sqlCommand.Parameters.AddWithValue("@procName", objectNameParser.ObjectName);
                sqlCommand.Parameters.AddWithValue("@schemaName", SqlManagementUtils.GetUnquotedSqlSchemaName(objectNameParser));
                if (transaction != null)
                {
                    sqlCommand.Transaction = transaction;
                }
                flag = (int)sqlCommand.ExecuteScalar() != 0;
            }
            return flag;
        }

 
        public static bool TableExists(SqlConnection connection, SqlTransaction transaction, string quotedTableName)
        {
            bool tableExist;
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTableName);
            using (DbCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "IF EXISTS (SELECT t.name FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.name = @tableName AND s.name = @schemaName) SELECT 1 ELSE SELECT 0";
                SqlParameter sqlParameter = new SqlParameter()
                {
                    ParameterName = "@tableName",
                    Value = objectNameParser.ObjectName
                };
                dbCommand.Parameters.Add(sqlParameter);
                sqlParameter = new SqlParameter()
                {
                    ParameterName = "@schemaName",
                    Value = SqlManagementUtils.GetUnquotedSqlSchemaName(objectNameParser)
                };
                dbCommand.Parameters.Add(sqlParameter);
                if (transaction != null)
                {
                    dbCommand.Transaction = transaction;
                }
                tableExist = (int)dbCommand.ExecuteScalar() != 0;
            }
            return tableExist;
        }

        public static bool TriggerExists(SqlConnection connection, SqlTransaction transaction, string quotedTriggerName)
        {
            bool triggerExist;
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTriggerName);
            using (SqlCommand sqlCommand = new SqlCommand("IF EXISTS (SELECT tr.name FROM sys.triggers tr JOIN sys.tables t ON tr.parent_id = t.object_id JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE tr.name = @triggerName and s.name = @schemaName) SELECT 1 ELSE SELECT 0", connection))
            {
                sqlCommand.Parameters.AddWithValue("@triggerName", objectNameParser.ObjectName);
                sqlCommand.Parameters.AddWithValue("@schemaName", SqlManagementUtils.GetUnquotedSqlSchemaName(objectNameParser));
                if (transaction != null)
                    sqlCommand.Transaction = transaction;

                triggerExist = (int)sqlCommand.ExecuteScalar() != 0;
            }
            return triggerExist;
        }

        public static bool TypeExists(SqlConnection connection, SqlTransaction transaction, string quotedTypeName)
        {
            bool typeExist;
            ObjectNameParser objectNameParser = new ObjectNameParser(quotedTypeName);
            using (SqlCommand sqlCommand = new SqlCommand("IF EXISTS (SELECT * FROM sys.types t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.name = @typeName AND s.name = @schemaName) SELECT 1 ELSE SELECT 0", connection))
            {
                sqlCommand.Parameters.AddWithValue("@typeName", objectNameParser.ObjectName);
                sqlCommand.Parameters.AddWithValue("@schemaName", SqlManagementUtils.GetUnquotedSqlSchemaName(objectNameParser));
                if (transaction != null)
                    sqlCommand.Transaction = transaction;

                typeExist = (int)sqlCommand.ExecuteScalar() != 0;
            }
            return typeExist;
        }
    }
}
