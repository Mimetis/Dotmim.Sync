
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.Manager
{
    public abstract class DbTableManagerFactory
    {

        public string TableName { get; }
        public string SchemaName { get; }

        public DbTableManagerFactory(string tableName, string schemaName)
        {
            this.TableName = tableName;
            this.SchemaName = schemaName;
        }

        /// <summary>
        /// Gets a table manager, who can execute somes queries directly on source database
        /// </summary>
        public abstract IDbTableManager CreateManagerTable(DbConnection connection, DbTransaction transaction = null);

        /// <summary>
        /// Get a parameter even if it's a @param or :param or param
        /// </summary>
        public static DbParameter GetParameter(DbCommand command, string parameterName)
        {
            if (command == null)
                return null;

            if (command.Parameters.Contains($"@{parameterName}"))
                return command.Parameters[$"@{parameterName}"];

            if (command.Parameters.Contains($":{parameterName}"))
                return command.Parameters[$":{parameterName}"];

            if (command.Parameters.Contains($"in_{parameterName}"))
                return command.Parameters[$"in_{parameterName}"];

            if (!command.Parameters.Contains(parameterName))
                return null;

            return command.Parameters[parameterName];
        }

        /// <summary>
        /// Set a parameter value
        /// </summary>
        public static void SetParameterValue(DbCommand command, string parameterName, object value)
        {
            var parameter = GetParameter(command, parameterName);
            if (parameter == null)
                return;

            if (value == null || value == DBNull.Value)
                parameter.Value = DBNull.Value;
            else
                parameter.Value = SyncTypeConverter.TryConvertFromDbType(value, parameter.DbType);


        }

        public static int GetSyncIntOutParameter(string parameter, DbCommand command)
        {
            DbParameter dbParameter = GetParameter(command, parameter);
            if (dbParameter == null || dbParameter.Value == null || string.IsNullOrEmpty(dbParameter.Value.ToString()))
                return 0;

            return int.Parse(dbParameter.Value.ToString(), CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Parse a time stamp value
        /// </summary>
        public static long ParseTimestamp(object obj)
        {
            if (obj == DBNull.Value)
                return 0;

            if (obj is long || obj is int || obj is ulong || obj is uint || obj is decimal)
                return Convert.ToInt64(obj, NumberFormatInfo.InvariantInfo);
            long timestamp;
            if (obj is string str)
            {
                long.TryParse(str, NumberStyles.HexNumber, CultureInfo.InvariantCulture.NumberFormat, out timestamp);
                return timestamp;
            }

            if (!(obj is byte[] numArray))
                return 0;

            var stringBuilder = new StringBuilder();
            for (int i = 0; i < numArray.Length; i++)
            {
                string str1 = numArray[i].ToString("X", NumberFormatInfo.InvariantInfo);
                stringBuilder.Append((str1.Length == 1 ? string.Concat("0", str1) : str1));
            }

            long.TryParse(stringBuilder.ToString(), NumberStyles.HexNumber, CultureInfo.InvariantCulture.NumberFormat, out timestamp);
            return timestamp;
        }



    }
}
