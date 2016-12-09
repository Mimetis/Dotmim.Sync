using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Core.Common
{

    /// <summary>
    /// Database Helper.
    /// Have to be agnostic to every relational datasources
    /// </summary>
    public static class DbHelper
    {

        /// <summary>
        /// Get a parameter even if it's a @param or :param or param
        /// </summary>
        public static DbParameter GetParameter(DbCommand command, string parameterName)
        {
            if (command == null)
                return null;

            if (command.Parameters.Contains(string.Concat("@", parameterName)))
                return command.Parameters[string.Concat("@", parameterName)];

            if (command.Parameters.Contains(string.Concat(":", parameterName)))
                return command.Parameters[string.Concat(":", parameterName)];

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
            if (parameter != null)
                if (value == null)
                    parameter.Value = DBNull.Value;
                else
                    parameter.Value = value;
        }


        internal static int GetSyncIntOutParameter(string parameter, DbCommand command)
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
        internal static long ParseTimestamp(object obj)
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

        /// <summary>
        /// Get a row size. Useful to calculate batch size, eventually
        /// </summary>
        internal static long GetRowSizeFromReader(IDataReader reader)
        {
            long bytes = (long)0;
            for (int i = 0; i < reader.FieldCount; i++)
            {
                Type fieldType = reader.GetFieldType(i);
                if (reader.IsDBNull(i))
                    bytes = bytes + 5;
                else if (fieldType == typeof(Guid))
                    bytes = bytes + 16;
                else if (fieldType != typeof(byte[]))
                    bytes = (fieldType != typeof(string) ? bytes + GetSizeForType(fieldType) : bytes + reader.GetChars(i, 0, null, 0, 0) * 2);
                else
                    bytes = bytes + reader.GetBytes(i, 0, null, 0, 0);
            }
            return bytes;
        }

        internal static long GetRowSizeFromDataRow(DmRow row)
        {
            bool isRowDeleted = false;
            if (row.RowState == DmRowState.Deleted)
            {
                row.RejectChanges();
                isRowDeleted = true;
            }

            long byteCount = 0;
            object[] itemArray = row.ItemArray;

            for (int i = 0; i < itemArray.Length; i++)
            {
                object obj = itemArray[i];
                string str = obj as string;
                byte[] numArray = obj as byte[];
                if (obj is DBNull)
                    byteCount = byteCount + 5;
                else if (obj is Guid)
                    byteCount = byteCount + 16;
                else if (str == null)
                    byteCount = (numArray == null ? byteCount + GetSizeForType(obj.GetType()) : byteCount + numArray.Length);
                else
                    byteCount = byteCount + Encoding.Unicode.GetByteCount(str);

            }
            if (isRowDeleted)
            {
                row.Delete();
            }
            return byteCount;
        }
        internal static long GetSizeForType(Type type)
        {

            if (type == typeof(object) || type == typeof(long) || type == typeof(ulong) ||
                type == typeof(double) || type == typeof(DateTime))
                return 8L;

            if (type == typeof(DBNull))
                return 0L;

            if (type == typeof(bool) || type == typeof(sbyte) || type == typeof(byte))
                return 1L;

            if (type == typeof(char) || type == typeof(short) || type == typeof(ushort))
                return 2L;

            if (type == typeof(int) || type == typeof(uint) || type == typeof(float))
                return 4L;

            if (type == typeof(decimal))
                return 16L;

            return 0L;

        }

        public static DbType GetDbTypeFromString(string typeString)
        {
            if (string.Compare(typeString, "numeric", StringComparison.OrdinalIgnoreCase) == 0)
                return DbType.Decimal;

            if (string.Equals(typeString, "uniqueidentifier", StringComparison.OrdinalIgnoreCase))
                return DbType.Guid;

            return (DbType)Enum.Parse(typeof(DbType), typeString, true);
        }
    }
}
