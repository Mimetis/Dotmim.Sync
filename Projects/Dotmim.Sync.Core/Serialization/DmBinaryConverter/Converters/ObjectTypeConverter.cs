using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Text;

namespace DmBinaryFormatter.Converters
{
    public class ObjectTypeConverter : ObjectConverter
    {

        public override string ConvertToString(Object obj)
        {
            return ((Type)obj).AssemblyQualifiedName;
        }

        public override object ConvertFromString(string s)
        {
            return Type.GetType(s);
        }

    }
}
