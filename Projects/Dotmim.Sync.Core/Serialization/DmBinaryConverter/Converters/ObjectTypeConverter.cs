using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Text;
using System.Runtime;
using System.Reflection;

namespace Dotmim.Sync.Serialization.Converters
{
    public class ObjectTypeConverter : ObjectConverter
    {

        public override string ConvertToString(Object obj)
        {
            var typeObj = (Type)obj;

            var typeCode = typeObj.GetAssemblyQualifiedName();

            return typeCode;
        }

        public override object ConvertFromString(string s)
        {
            return DmUtils.GetTypeFromAssemblyQualifiedName(s);
        }

    }
}
