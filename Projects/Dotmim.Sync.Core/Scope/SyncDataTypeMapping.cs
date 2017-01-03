using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Core.Scope
{
    /// <summary>
    /// TODO : Merge with the correct helper, in Sql Provider
    /// </summary>
    public static class SyncDataTypeMapping
    {
        static Dictionary<string, Type> DataTypeHashtable;

        public static Type GetType(string sqlTypeName)
        {
            if (DataTypeHashtable == null)
                Init();

            return DataTypeHashtable[sqlTypeName.Trim()];
        }

        static void Init()
        {
            DataTypeHashtable = new Dictionary<string, Type>
            {
                { "bigint", Type.GetType("System.Int64") },
                { "binary", Type.GetType("System.Byte[]") },
                { "bit", Type.GetType("System.Boolean") },
                { "char", Type.GetType("System.String") },
                { "datetime", Type.GetType("System.DateTime") },
                { "decimal", Type.GetType("System.Decimal") },
                { "float", Type.GetType("System.Double") },
                { "image", Type.GetType("System.Byte[]") },
                { "int", Type.GetType("System.Int32") },
                { "money", Type.GetType("System.Decimal") },
                { "nchar", Type.GetType("System.String") },
                { "ntext", Type.GetType("System.String") },
                { "numeric", Type.GetType("System.Decimal") },
                { "nvarchar", Type.GetType("System.String") },
                { "real", Type.GetType("System.Single") },
                { "smalldatetime", Type.GetType("System.DateTime") },
                { "smallint", Type.GetType("System.Int16") },
                { "smallmoney", Type.GetType("System.Decimal") },
                { "text", Type.GetType("System.String") },
                { "timestamp", Type.GetType("System.Byte[]") },
                { "rowversion", Type.GetType("System.Byte[]") },
                { "tinyint", Type.GetType("System.Byte") },
                { "uniqueidentifier", Type.GetType("System.Guid") },
                { "varbinary", Type.GetType("System.Byte[]") },
                { "varchar", Type.GetType("System.String") },
                { "xml", Type.GetType("System.String") },
                { "sql_variant", Type.GetType("System.Object") }
            };
        }
    }
}
