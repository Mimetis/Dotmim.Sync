using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Text;

namespace DmBinaryFormatter.Converters
{
    public class CultureInfoConverter : ObjectConverter
    {

        public override string ConvertToString(Object obj)
        {
            var ci = (CultureInfo)obj;

            return ci.Name;
        }

        public override object ConvertFromString(string s)
        {
            CultureInfo ci = new CultureInfo(s);
            return ci;
        }

    }
}
