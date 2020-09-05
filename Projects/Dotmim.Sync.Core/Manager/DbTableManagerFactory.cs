
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

            // OBSOLETE : Wait for tests to pass and then, jsut remove
            //if (value != null && value != DBNull.Value)
            //{
            //    var columnType = parameter.DbType;
            //    var valueType = value.GetType();

            //    try
            //    {
            //        if (columnType == DbType.Guid && valueType != typeof(Guid) && (value as string) != null)
            //            value = new Guid(value.ToString());
            //        else if (columnType == DbType.Guid && valueType != typeof(Guid) && value.GetType() == typeof(byte[]))
            //            value = new Guid((byte[])value);
            //        else if (columnType == DbType.Int32 && valueType != typeof(int))
            //            value = Convert.ToInt32(value);
            //        else if (columnType == DbType.UInt32 && valueType != typeof(uint))
            //            value = Convert.ToUInt32(value);
            //        else if (columnType == DbType.UInt16 && valueType != typeof(short))
            //            value = Convert.ToInt16(value);
            //        else if (columnType == DbType.UInt16 && valueType != typeof(ushort))
            //            value = Convert.ToUInt16(value);
            //        else if (columnType == DbType.Int64 && valueType != typeof(long))
            //            value = Convert.ToInt64(value);
            //        else if (columnType == DbType.UInt64 && valueType != typeof(ulong))
            //            value = Convert.ToUInt64(value);
            //        else if (columnType == DbType.Byte && valueType != typeof(byte))
            //            value = Convert.ToByte(value);
            //        else if (columnType == DbType.Currency && valueType != typeof(Decimal))
            //            value = Convert.ToDecimal(value);
            //        else if (columnType == DbType.DateTime && valueType != typeof(DateTime))
            //            value = Convert.ToDateTime(value);
            //        else if (columnType == DbType.DateTime2 && valueType != typeof(DateTime))
            //            value = Convert.ToDateTime(value);
            //        else if (columnType == DbType.DateTimeOffset && valueType != typeof(DateTimeOffset))
            //            value = SyncTypeConverter.TryConvertTo<DateTimeOffset>(value);
            //        else if (columnType == DbType.Decimal && valueType != typeof(decimal))
            //            value = Convert.ToDecimal(value);
            //        else if (columnType == DbType.Double && valueType != typeof(double))
            //            value = Convert.ToDouble(value);
            //        else if (columnType == DbType.SByte && valueType != typeof(sbyte))
            //            value = Convert.ToSByte(value);
            //        else if (columnType == DbType.VarNumeric && valueType != typeof(float))
            //            value = Convert.ToSingle(value);
            //        else if (columnType == DbType.String && valueType != typeof(string))
            //            value = Convert.ToString(value);
            //        else if (columnType == DbType.StringFixedLength && valueType != typeof(string))
            //            value = Convert.ToString(value);
            //        else if (columnType == DbType.AnsiString && valueType != typeof(string))
            //            value = Convert.ToString(value);
            //        else if (columnType == DbType.AnsiStringFixedLength && valueType != typeof(string))
            //            value = Convert.ToString(value);
            //        else if (columnType == DbType.Boolean && valueType != typeof(bool))
            //            value = Convert.ToBoolean(value);
            //    }
            //    catch
            //    {
            //        // if execption, just try to set the value, directly
            //    }
            //}
            //parameter.Value = value;
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
