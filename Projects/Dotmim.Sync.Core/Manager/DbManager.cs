using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.Manager
{
    public abstract class DbManager
    {

        public string TableName { get; }

        public DbManager(string tableName)
        {
            this.TableName = tableName;
        }

        /// <summary>
        /// Gets a table manager, who can execute somes queries directly on source database
        /// </summary>
        public abstract IDbManagerTable CreateManagerTable(DbConnection connection, DbTransaction transaction = null);



        public IDbManagerTable GetManagerTable(DbConnection connection, DbTransaction transaction = null)
        {
            var mgerTable = CreateManagerTable(connection, transaction);
            mgerTable.TableName = this.TableName;
            return mgerTable;
        }

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

            if (command.Parameters.Contains($"in{parameterName}"))
                return command.Parameters[$"in{parameterName}"];

            if (!command.Parameters.Contains(parameterName))
                return null;

            return command.Parameters[parameterName];
        }

        /// <summary>
        /// Set a parameter value
        /// </summary>
        public static void SetParameterValue(DbCommand command, string parameterName, object value)
        {
            DbParameter parameter = GetParameter(command, parameterName);
            if (parameter == null)
                return;

            parameter.Value = value == null ? DBNull.Value : value;
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
        /// <param name="obj"></param>
        /// <returns></returns>
        public static long ParseTimestamp(object obj)
        {
            long timestamp = 0;

            if (obj == DBNull.Value)
                return 0;

            if (obj is long || obj is int || obj is ulong || obj is uint || obj is decimal)
                return Convert.ToInt64(obj, NumberFormatInfo.InvariantInfo);

            string str = obj as string;
            if (str != null)
            {
                long.TryParse(str, NumberStyles.HexNumber, CultureInfo.InvariantCulture.NumberFormat, out timestamp);
                return timestamp;
            }

            byte[] numArray = obj as byte[];
            if (numArray == null)
                return 0;

            StringBuilder stringBuilder = new StringBuilder();
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
