using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace Dotmim.Sync
{
    public class SyncTypeConverter
    {
        public static T TryConvertTo<T>(dynamic value, CultureInfo provider = default)
        {
            if (value == null)
                return default;

            provider = provider ?? CultureInfo.InvariantCulture;

            var typeOfT = typeof(T);
            var typeOfU = value.GetType();

            if (typeOfT == typeOfU)
                return (T)Convert.ChangeType(value, typeOfT, provider);

            var typeConverter = TypeDescriptor.GetConverter(typeOfT);

            if (typeOfT == typeof(short))
                return Convert.ToInt16(value);
            else if (typeOfT == typeof(int))
                return Convert.ToInt32(value);
            else if (typeOfT == typeof(long))
                return Convert.ToInt64(value);
            else if (typeOfT == typeof(ushort))
                return Convert.ToUInt16(value);
            else if (typeOfT == typeof(uint))
                return Convert.ToUInt32(value);
            else if (typeOfT == typeof(ulong))
                return Convert.ToUInt64(value);
            else if (typeOfT == typeof(DateTime))
            {
                if (DateTime.TryParse(value.ToString(), provider, DateTimeStyles.None, out DateTime dateTime))
                    return (T)Convert.ChangeType(dateTime, typeOfT, provider);
                else if (typeOfU == typeof(long))
                    return (T)Convert.ChangeType(new DateTime(value), typeOfT, provider);
                else
                    return Convert.ToDateTime(value);
            }
            else if (typeOfT == typeof(DateTimeOffset))
            {
                if (DateTimeOffset.TryParse(value.ToString(), provider, DateTimeStyles.None, out DateTimeOffset dateTime))
                    return (T)Convert.ChangeType(dateTime, typeOfT, provider);
                else if (typeOfU == typeof(long))
                    return (T)Convert.ChangeType(new DateTimeOffset(new DateTime(value)), typeOfT, provider);
                else
                    return Convert.ToDateTime(value);
            }
            else if (typeOfT == typeof(string))
                return value.ToString();
            else if (typeOfT == typeof(byte))
                return Convert.ToByte(value);
            else if (typeOfT == typeof(bool))
            {
                if (bool.TryParse(value.ToString(), out bool v))
                    return (T)Convert.ChangeType(v, typeOfT);
                else if (value.ToString().Trim() == "0")
                    return (T)Convert.ChangeType(false, typeOfT, provider);
                else if (value.ToString().Trim() == "1")
                    return (T)Convert.ChangeType(true, typeOfT, provider);
                else
                    return Convert.ToBoolean(value);
            }
            else if (typeOfT == typeof(Guid))
            {
                string valueStr = value.ToString();
                if (Guid.TryParse(valueStr, out Guid j))
                    return (T)Convert.ChangeType(j, typeOfT, provider);
                else if (value.GetType() == typeof(byte[]))
                    return (T)Convert.ChangeType(new Guid(value as byte[]), typeOfT, provider);
                else
                    return (T)Convert.ChangeType(new Guid(value.ToString()), typeOfT, provider);
            }
            else if (typeOfT == typeof(char))
                return Convert.ToChar(value);
            else if (typeOfT == typeof(decimal))
                return Convert.ToDecimal(value, provider);
            else if (typeOfT == typeof(double))
                return Convert.ToDouble(value, provider.NumberFormat);
            else if (typeOfT == typeof(float))
                return Convert.ToSingle(value, provider.NumberFormat);
            else if (typeOfT == typeof(sbyte))
                return Convert.ToSByte(value);
            else if (typeOfT == typeof(TimeSpan))
            {
                if (typeOfU == typeof(Int16) || typeOfU == typeof(Int32) || typeOfU == typeof(Int64)
                   || typeOfU == typeof(UInt16) || typeOfU == typeof(UInt32) || typeOfU == typeof(UInt64))
                    return TimeSpan.FromTicks(value);
                if (TimeSpan.TryParse(value.ToString(), provider, out TimeSpan q))
                    return (T)Convert.ChangeType(q, typeOfT, provider);
            }
            else if (typeOfT == typeof(byte[]))
            {
                if (typeOfU == typeof(string))
                    return (T)Convert.ChangeType(Convert.FromBase64String((string)value), typeOfT, provider);
                else
                    return (T)Convert.ChangeType(BitConverter.GetBytes((dynamic)value), typeOfT, provider);
            }
            else if (typeConverter.CanConvertFrom(typeOfT))
                return (T)Convert.ChangeType(typeConverter.ConvertFrom(value), typeOfT, provider);
            else
                throw new FormatTypeException(typeOfT);

            return default;
        }

        public static object TryConvertTo(object value, Type typeOfT, CultureInfo provider = default)
        {
            var typeConverter = TypeDescriptor.GetConverter(typeOfT);

            if (typeOfT == typeof(short))
                return TryConvertTo<short>(value, provider);
            else if (typeOfT == typeof(int))
                return TryConvertTo<int>(value, provider);
            else if (typeOfT == typeof(long))
                return TryConvertTo<long>(value, provider);
            else if (typeOfT == typeof(ushort))
                return TryConvertTo<ushort>(value, provider);
            else if (typeOfT == typeof(uint))
                return TryConvertTo<uint>(value, provider);
            else if (typeOfT == typeof(ulong))
                return TryConvertTo<ulong>(value, provider);
            else if (typeOfT == typeof(DateTime))
                return TryConvertTo<DateTime>(value, provider);
            else if (typeOfT == typeof(DateTimeOffset))
                return TryConvertTo<DateTimeOffset>(value, provider);
            else if (typeOfT == typeof(string))
                return TryConvertTo<string>(value, provider);
            else if (typeOfT == typeof(byte))
                return TryConvertTo<byte>(value, provider);
            else if (typeOfT == typeof(bool))
                return TryConvertTo<bool>(value, provider);
            else if (typeOfT == typeof(Guid))
                return TryConvertTo<Guid>(value, provider);
            else if (typeOfT == typeof(char))
                return TryConvertTo<char>(value, provider);
            else if (typeOfT == typeof(decimal))
                return TryConvertTo<decimal>(value, provider);
            else if (typeOfT == typeof(double))
                return TryConvertTo<double>(value, provider);
            else if (typeOfT == typeof(float))
                return TryConvertTo<float>(value, provider);
            else if (typeOfT == typeof(sbyte))
                return TryConvertTo<sbyte>(value, provider);
            else if (typeOfT == typeof(TimeSpan))
                return TryConvertTo<TimeSpan>(value, provider);
            else if (typeOfT == typeof(byte[]))
                return TryConvertTo<byte[]>(value, provider);
            else if (typeConverter.CanConvertFrom(typeOfT))
                return Convert.ChangeType(typeConverter.ConvertFrom(value), typeOfT, provider);
            else if (typeOfT == typeof(Object))
                return value;
            else
                throw new FormatTypeException(typeOfT);

        }


        public static object TryConvertFromDbType(object value, DbType typeOfT, CultureInfo provider = default)
        {
            if (typeOfT == DbType.AnsiString || typeOfT == DbType.String
                || typeOfT == DbType.StringFixedLength || typeOfT == DbType.AnsiStringFixedLength
                || typeOfT == DbType.Xml)
                return TryConvertTo<string>(value, provider);
            else if (typeOfT == DbType.Binary)
                return TryConvertTo<byte[]>(value, provider);
            else if (typeOfT == DbType.Boolean)
                return TryConvertTo<bool>(value, provider);
            else if (typeOfT == DbType.Byte)
                return TryConvertTo<byte>(value, provider);
            else if (typeOfT == DbType.Currency || typeOfT == DbType.Decimal)
                return TryConvertTo<decimal>(value, provider);
            else if (typeOfT == DbType.Date || typeOfT == DbType.DateTime
                    || typeOfT == DbType.DateTime2 || typeOfT == DbType.DateTimeOffset)
                return TryConvertTo<DateTime>(value, provider);
            else if (typeOfT == DbType.Double)
                return TryConvertTo<double>(value, provider);
            else if (typeOfT == DbType.Guid)
                return TryConvertTo<Guid>(value, provider);
            else if (typeOfT == DbType.Int16)
                return TryConvertTo<short>(value, provider);
            else if (typeOfT == DbType.Int32)
                return TryConvertTo<int>(value, provider);
            else if (typeOfT == DbType.Int64)
                return TryConvertTo<long>(value, provider);
            else if (typeOfT == DbType.SByte)
                return TryConvertTo<sbyte>(value, provider);
            else if (typeOfT == DbType.Single)
                return TryConvertTo<float>(value, provider);
            else if (typeOfT == DbType.Time)
                return TryConvertTo<TimeSpan>(value, provider);
            else if (typeOfT == DbType.UInt16)
                return TryConvertTo<ushort>(value, provider);
            else if (typeOfT == DbType.UInt32)
                return TryConvertTo<uint>(value, provider);
            else if (typeOfT == DbType.UInt64)
                return TryConvertTo<ulong>(value, provider);
            else if (typeOfT == DbType.VarNumeric)
                return TryConvertTo<float>(value, provider);
            else if (typeOfT == DbType.Object)
                return TryConvertTo<byte[]>(value, provider);
            else
                throw new FormatDbTypeException(typeOfT);

        }


    }
}
