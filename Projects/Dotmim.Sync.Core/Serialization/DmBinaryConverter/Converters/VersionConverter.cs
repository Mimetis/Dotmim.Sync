using System;
using System.Collections.Generic;
using System.Text;

namespace DmBinaryFormatter.Converters
{
    public class VersionConverter : ObjectConverter
    {

        public override string ConvertToString(Object obj)
        {
            var v = (Version)obj;

            return v.ToString();
        }

        public override object ConvertFromString(string s)
        {
            Version v = new Version(s);
            return v;
        }
    }
}
