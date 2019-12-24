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


        /// <summary>
        /// Try to convert with 2 methods : ChangeType and TypeConverter
        /// </summary>
        public static bool TryConvertTo<T>(object o, out object res)
        {
            res = default;
            var cul = CultureInfo.InvariantCulture;
            var num = NumberStyles.Any;
            var typeOfT = typeof(T);
            var typeConverter = TypeDescriptor.GetConverter(typeOfT);

            //var typeCode = Type.GetTypeCode(typeOfT);
            //// Dont try to change to type object
            //if (typeCode != TypeCode.Object)
            //{
            //    try
            //    {
            //        res = (T)Convert.ChangeType(o, typeCode);
            //        return true;
            //    }
            //    catch
            //    {
            //    }
            //}

            if (typeOfT == typeof(short) && short.TryParse(o.ToString(), num, cul, out var a))
                res = a;
            else if (typeOfT == typeof(int) && int.TryParse(o.ToString(), num, cul, out var b))
                res = b;
            else if (typeOfT == typeof(long) && long.TryParse(o.ToString(), num, cul, out var c))
                res = c;
            else if (typeOfT == typeof(ushort) && ushort.TryParse(o.ToString(), num, cul, out var d))
                res = d;
            else if (typeOfT == typeof(uint) && uint.TryParse(o.ToString(), num, cul, out var e))
                res = e;
            else if (typeOfT == typeof(ulong) && ulong.TryParse(o.ToString(), num, cul, out var f))
                res = f;
            else if (typeOfT == typeof(DateTime) && DateTime.TryParse(o.ToString(), cul, DateTimeStyles.None, out var g))
                res = g;
            else if (typeOfT == typeof(string))
                res = o.ToString();
            else if (typeOfT == typeof(byte) && byte.TryParse(o.ToString(), num, cul, out var h))
                res = h;
            else if (typeOfT == typeof(bool) && bool.TryParse(o.ToString(), out var i))
                res = i;
            else if (typeOfT == typeof(Guid))
            {
                if (Guid.TryParse(o.ToString(), out var j))
                    res = j;
                else if (o.GetType() == typeof(byte[]))
                    res = new Guid((byte[])o);
                else
                    res = new Guid(o.ToString());
            }
            else if (typeOfT == typeof(char) && char.TryParse(o.ToString(), out var k))
                res = k;
            else if (typeOfT == typeof(decimal) && decimal.TryParse(o.ToString(), num, cul, out var l))
                res = l;
            else if (typeOfT == typeof(double) && double.TryParse(o.ToString(), num, cul, out var m))
                res = m;
            else if (typeOfT == typeof(float) && float.TryParse(o.ToString(), num, cul, out var n))
                res = n;
            else if (typeOfT == typeof(sbyte) && sbyte.TryParse(o.ToString(), num, cul, out var p))
                res = p;
            else if (typeOfT == typeof(TimeSpan) && TimeSpan.TryParse(o.ToString(), cul, out var q))
                res = q;
            else if (typeConverter.CanConvertFrom(typeOfT))
                res = (T)typeConverter.ConvertFrom(o);
            else
                return false;

            return true;
        }



        private static TypeConverter Int16Converter = TypeDescriptor.GetConverter(typeof(short));
        private static TypeConverter Int32Converter = TypeDescriptor.GetConverter(typeof(int));
        private static TypeConverter Int64Converter = TypeDescriptor.GetConverter(typeof(long));
        private static TypeConverter UInt16Converter = TypeDescriptor.GetConverter(typeof(ushort));
        private static TypeConverter UInt32Converter = TypeDescriptor.GetConverter(typeof(uint));
        private static TypeConverter UInt64Converter = TypeDescriptor.GetConverter(typeof(ulong));
        private static TypeConverter DateTimeConverter = TypeDescriptor.GetConverter(typeof(DateTime));
        private static TypeConverter StringConverter = TypeDescriptor.GetConverter(typeof(string));
        private static TypeConverter ByteConverter = TypeDescriptor.GetConverter(typeof(byte));
        private static TypeConverter BoolConverter = TypeDescriptor.GetConverter(typeof(bool));
        private static TypeConverter GuidConverter = TypeDescriptor.GetConverter(typeof(Guid));
        private static TypeConverter CharConverter = TypeDescriptor.GetConverter(typeof(char));
        private static TypeConverter DecimalConverter = TypeDescriptor.GetConverter(typeof(decimal));
        private static TypeConverter DoubleConverter = TypeDescriptor.GetConverter(typeof(double));
        private static TypeConverter FloatConverter = TypeDescriptor.GetConverter(typeof(float));
        private static TypeConverter SByteConverter = TypeDescriptor.GetConverter(typeof(sbyte));
        private static TypeConverter TimeSpanConverter = TypeDescriptor.GetConverter(typeof(TimeSpan));


    }
}
