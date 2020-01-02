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

        public static bool TryConvertTo(object o, Type typeOfT, out object res)
        {
            res = default;
            var cul = CultureInfo.InvariantCulture;

            var typeConverter = TypeDescriptor.GetConverter(typeOfT);

            var nfi = new NumberFormatInfo
            {
                NumberDecimalSeparator = SyncGlobalization.DataSourceNumberDecimalSeparator
            };

            if (typeOfT == typeof(short))
                res = Convert.ToInt16(o);
            else if (typeOfT == typeof(int))
                res = Convert.ToInt32(o);
            else if (typeOfT == typeof(long))
                res = Convert.ToInt64(o);
            else if (typeOfT == typeof(ushort))
                res = Convert.ToUInt16(o);
            else if (typeOfT == typeof(uint))
                res = Convert.ToUInt32(o);
            else if (typeOfT == typeof(ulong))
                res = Convert.ToUInt64(o);
            else if (typeOfT == typeof(DateTime))
                res = Convert.ToDateTime(o);
            else if (typeOfT == typeof(string))
                res = o.ToString();
            else if (typeOfT == typeof(byte))
                res = Convert.ToByte(o);
            else if (typeOfT == typeof(bool) && bool.TryParse(o.ToString(), out var i))
                res = Convert.ToBoolean(o);
            else if (typeOfT == typeof(Guid))
            {
                if (Guid.TryParse(o.ToString(), out var j))
                    res = j;
                else if (o.GetType() == typeof(byte[]))
                    res = new Guid((byte[])o);
                else
                    res = new Guid(o.ToString());
            }
            else if (typeOfT == typeof(char))
                res = Convert.ToChar(o);
            else if (typeOfT == typeof(decimal))
                    res = Convert.ToDecimal(o, nfi);
            else if (typeOfT == typeof(double))
                    res = Convert.ToDecimal(o, nfi);
            else if (typeOfT == typeof(float))
                    res = Convert.ToDecimal(o, nfi);
            else if (typeOfT == typeof(sbyte))
                res = Convert.ToSByte(o);
            else if (typeOfT == typeof(TimeSpan) && TimeSpan.TryParse(o.ToString(), cul, out var q))
                res = q;
            else if (typeConverter.CanConvertFrom(typeOfT))
                res = typeConverter.ConvertFrom(o);
            else
                return false;

            return true;
        }

        /// <summary>
        /// Try to convert with 2 methods : ChangeType and TypeConverter
        /// </summary>
        public static bool TryConvertTo<T>(object o, out object res) => TryConvertTo(o, typeof(T), out res);




    }
}
