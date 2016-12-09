using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Core.Common
{
    public static class ExtensionStringMethods
    {
        public static string ToEscapeString(this string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return string.Empty;
            }
            return value.Replace("'", "''");
        }
    }

    
}
