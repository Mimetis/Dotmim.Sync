using System;
using System.ComponentModel;
using System.Data;
using System.Globalization;

namespace Dotmim.Sync
{
    /// <summary>
    /// Sync Type Converter: Convert a value to another type.
    /// </summary>
    public static class SyncTypeConverter
    {
        /// <summary>
        /// Try to convert a value to another type.
        /// </summary>
        public static T TryConvertTo<T>(dynamic value, CultureInfo provider = default)
        {
            if (value == null)
                return default;

            provider ??= CultureInfo.InvariantCulture;

            var typeOfT = typeof(T);
            var typeOfU = value.GetType();

            if (typeOfT == typeOfU)
                return (T)Convert.ChangeType(value, typeOfT, provider);

            var typeConverter = TypeDescriptor.GetConverter(typeOfT);

            if (typeOfT == typeof(short))
            {
                return Convert.ToInt16(value, provider);
            }
            else if (typeOfT == typeof(int))
            {
                return Convert.ToInt32(value);
            }
            else if (typeOfT == typeof(long))
            {
                return Convert.ToInt64(value);
            }
            else if (typeOfT == typeof(ushort))
            {
                return Convert.ToUInt16(value);
            }
            else if (typeOfT == typeof(uint))
            {
                return Convert.ToUInt32(value);
            }
            else if (typeOfT == typeof(ulong))
            {
                return Convert.ToUInt64(value);
            }
#if NET6_0_OR_GREATER
            else if (typeOfT == typeof(DateOnly))
            {
                if (value is DateTimeOffset dateTimeOffset)
                    return (T)Convert.ChangeType(DateOnly.FromDateTime(dateTimeOffset.DateTime), typeOfT, provider);

                string valueStr = value.ToString(); // IOS bug ????
                if (DateOnly.TryParse(valueStr, provider, DateTimeStyles.None, out DateOnly dateOnly))
                    return (T)Convert.ChangeType(dateOnly, typeOfT, provider);
                else if (typeOfU == typeof(long))
                    return (T)Convert.ChangeType(DateOnly.FromDateTime(new DateTime(value)), typeOfT, provider);
                else
                    return (T)Convert.ChangeType(DateOnly.FromDateTime(Convert.ToDateTime(value)), typeOfT, provider);
            }

#endif
            else if (typeOfT == typeof(DateTime))
            {
                if (value is DateTimeOffset dateTimeOffset)
                    return (T)Convert.ChangeType(dateTimeOffset.DateTime, typeOfT, provider);

                string valueStr = value.ToString(); // IOS bug ????
                if (DateTime.TryParse(valueStr, provider, DateTimeStyles.None, out DateTime dateTime))
                    return (T)Convert.ChangeType(dateTime, typeOfT, provider);
                else if (typeOfU == typeof(long))
                    return (T)Convert.ChangeType(new DateTime(value), typeOfT, provider);
                else
                    return Convert.ToDateTime(value);
            }
            else if (typeOfT == typeof(DateTimeOffset))
            {
                if (value is DateTime dateTime)
                    return (T)Convert.ChangeType(new DateTimeOffset(dateTime), typeOfT, provider);
                else if (DateTimeOffset.TryParse(value.ToString(), provider, DateTimeStyles.None, out DateTimeOffset dateTimeOffset))
                    return (T)Convert.ChangeType(dateTimeOffset, typeOfT, provider);
                else if (typeOfU == typeof(long))
                    return (T)Convert.ChangeType(new DateTimeOffset(new DateTime(value)), typeOfT, provider);
                else
                    return Convert.ToDateTime(value);
            }
            else if (typeOfT == typeof(string))
            {
                return value.ToString();
            }
            else if (typeOfT == typeof(byte))
            {
                return Convert.ToByte(value);
            }
            else if (typeOfT == typeof(bool))
            {
                if (bool.TryParse(value.ToString(), out bool v))
                    return (T)Convert.ChangeType(v, typeOfT, CultureInfo.InvariantCulture);
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
            {
                return Convert.ToChar(value);
            }
            else if (typeOfT == typeof(decimal))
            {
                return Convert.ToDecimal(value, provider);
            }
            else if (typeOfT == typeof(double))
            {
                return Convert.ToDouble(value, provider.NumberFormat);
            }
            else if (typeOfT == typeof(float))
            {
                return Convert.ToSingle(value, provider.NumberFormat);
            }
            else if (typeOfT == typeof(sbyte))
            {
                return Convert.ToSByte(value);
            }
            else if (typeOfT == typeof(TimeSpan))
            {
                if (typeOfU == typeof(short) || typeOfU == typeof(int) || typeOfU == typeof(long)
                   || typeOfU == typeof(ushort) || typeOfU == typeof(uint) || typeOfU == typeof(ulong))
                    return (T)Convert.ChangeType(TimeSpan.FromTicks(value), typeOfT, provider);
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
            {
                return (T)Convert.ChangeType(typeConverter.ConvertFrom(value), typeOfT, provider);
            }
            else
            {
                throw new FormatTypeException(typeOfT);
            }

            return default;
        }

        /// <summary>
        /// Try to convert a value to another type.
        /// </summary>
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
#if NET6_0_OR_GREATER
            else if (typeOfT == typeof(DateOnly))
                return TryConvertTo<DateOnly>(value, provider);
#endif
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
            else if (typeOfT == typeof(object))
                return value;
            else
                throw new FormatTypeException(typeOfT);
        }

        /// <summary>
        /// Try to convert a value from DbType to another type.
        /// </summary>
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
            else if (typeOfT == DbType.Date)
#if NET6_0_OR_GREATER
                return TryConvertTo<DateOnly>(value, provider);
#else
                return TryConvertTo<DateTime>(value, provider);
#endif
            else if (typeOfT == DbType.DateTime || typeOfT == DbType.DateTime2)
                return TryConvertTo<DateTime>(value, provider);
            else if (typeOfT == DbType.DateTimeOffset)
                return TryConvertTo<DateTimeOffset>(value, provider);
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