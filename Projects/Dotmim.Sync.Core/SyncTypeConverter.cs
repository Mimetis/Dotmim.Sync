using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace Dotmim.Sync
{
    public class SyncTypeConverter
    {
        public static T TryConvertTo<T>(dynamic value, NumberFormatInfo nfi = null)
        {
            var cul = CultureInfo.InvariantCulture;

            if (value == null)
                return default;

            var typeOfT = typeof(T);
            var typeOfU = value.GetType();

            if (typeOfT == typeOfU)
                return (T)Convert.ChangeType(value, typeOfT);

            var typeConverter = TypeDescriptor.GetConverter(typeOfT);

            if (nfi == null)
            {
                nfi = new NumberFormatInfo
                {
                    NumberDecimalSeparator = SyncGlobalization.DataSourceNumberDecimalSeparator
                };
            }

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
                if (DateTime.TryParse(value.ToString(), out DateTime dateTime))
                    return (T)Convert.ChangeType(dateTime, typeOfT);
                else if (typeOfU == typeof(long))
                    return (T)Convert.ChangeType(new DateTime(value), typeOfT);
                else
                    return Convert.ToDateTime(value);
            }
            else if (typeOfT == typeof(DateTimeOffset))
            {
                if (DateTimeOffset.TryParse(value.ToString(), out DateTimeOffset dateTime))
                    return (T)Convert.ChangeType(dateTime, typeOfT);
                else if (typeOfU == typeof(long))
                    return (T)Convert.ChangeType(new DateTimeOffset(new DateTime(value)), typeOfT);
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
                    return (T)Convert.ChangeType(false, typeOfT);
                else if (value.ToString().Trim() == "1")
                    return (T)Convert.ChangeType(true, typeOfT);
                else
                    return Convert.ToBoolean(value);
            }
            else if (typeOfT == typeof(Guid))
            {
                if (Guid.TryParse(value.ToString(), out Guid j))
                    return (T)Convert.ChangeType(j, typeOfT);
                else if (value.GetType() == typeof(byte[]))
                    return (T)Convert.ChangeType(new Guid(value as byte[]), typeOfT);
                else
                    return (T)Convert.ChangeType(new Guid(value.ToString()), typeOfT);
            }
            else if (typeOfT == typeof(char))
                return Convert.ToChar(value);
            else if (typeOfT == typeof(decimal))
                return Convert.ToDecimal(value, nfi);
            else if (typeOfT == typeof(double))
                return Convert.ToDouble(value, nfi);
            else if (typeOfT == typeof(float))
                return Convert.ToSingle(value, nfi);
            else if (typeOfT == typeof(sbyte))
                return Convert.ToSByte(value);
            else if (typeOfT == typeof(TimeSpan))
            {
                if (typeOfU == typeof(Int16) || typeOfU == typeof(Int32) || typeOfU == typeof(Int64)
                   || typeOfU == typeof(UInt16) || typeOfU == typeof(UInt32) || typeOfU == typeof(UInt64))
                    return TimeSpan.FromTicks(value);
                if (TimeSpan.TryParse(value.ToString(), cul, out TimeSpan q))
                    return (T)Convert.ChangeType(q, typeOfT);
            }
            else if (typeOfT == typeof(byte[]))
            {
                if (typeOfU == typeof(string))
                    return (T)Convert.ChangeType(Convert.FromBase64String((string)value), typeOfT);
                else
                    return (T)Convert.ChangeType(BitConverter.GetBytes((dynamic)value), typeOfT);
            }
            else if (typeConverter.CanConvertFrom(typeOfT))
                return (T)Convert.ChangeType(typeConverter.ConvertFrom(value), typeOfT);
            else
                throw new FormatTypeException(typeOfT);

            return default;
        }

        public static object TryConvertTo(object value, Type typeOfT)
        {
            var typeConverter = TypeDescriptor.GetConverter(typeOfT);

            if (typeOfT == typeof(short))
                return TryConvertTo<short>(value);
            else if (typeOfT == typeof(int))
                return TryConvertTo<int>(value);
            else if (typeOfT == typeof(long))
                return TryConvertTo<long>(value);
            else if (typeOfT == typeof(ushort))
                return TryConvertTo<ushort>(value);
            else if (typeOfT == typeof(uint))
                return TryConvertTo<uint>(value);
            else if (typeOfT == typeof(ulong))
                return TryConvertTo<ulong>(value);
            else if (typeOfT == typeof(DateTime))
                return TryConvertTo<DateTime>(value);
            else if (typeOfT == typeof(DateTimeOffset))
                return TryConvertTo<DateTimeOffset>(value);
            else if (typeOfT == typeof(string))
                return TryConvertTo<string>(value);
            else if (typeOfT == typeof(byte))
                return TryConvertTo<byte>(value);
            else if (typeOfT == typeof(bool))
                return TryConvertTo<bool>(value);
            else if (typeOfT == typeof(Guid))
                return TryConvertTo<Guid>(value);
            else if (typeOfT == typeof(char))
                return TryConvertTo<char>(value);
            else if (typeOfT == typeof(decimal))
                return TryConvertTo<decimal>(value);
            else if (typeOfT == typeof(double))
                return TryConvertTo<double>(value);
            else if (typeOfT == typeof(float))
                return TryConvertTo<float>(value);
            else if (typeOfT == typeof(sbyte))
                return TryConvertTo<sbyte>(value);
            else if (typeOfT == typeof(TimeSpan))
                return TryConvertTo<TimeSpan>(value);
            else if (typeOfT == typeof(byte[]))
                return TryConvertTo<byte[]>(value);
            else if (typeConverter.CanConvertFrom(typeOfT))
                return Convert.ChangeType(typeConverter.ConvertFrom(value), typeOfT);
            else
                throw new FormatTypeException(typeOfT);

        }




    }
}
