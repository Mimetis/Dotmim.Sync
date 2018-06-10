using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Manager
{
    public class DbConverterMySql : DbConverter
    {

        public override bool IsValid(string type)
        {
            switch (type)
            {
                case "blob":
                case "mediumblob":
                case "longblob":
                case "binary":
                case "varbinary":
                case "tinyblob":
                case "date":
                case "datetime":
                case "double":
                case "float":
                case "decimal":
                case "int":
                case "integer":
                case "mediumint":
                case "bigint":
                case "timestamp":
                case "tinyint":
                case "year":
                case "bit":
                case "char":
                case "nchar":
                case "longtext":
                case "mediumtext":
                case "varchar":
                case "nvarchar":
                case "text":
                case "tinytext":
                    return true;
            }
            return false;
        }
        /// <summary>
        /// Convert a Sql column to MySql column
        /// </summary>
        private static DmColumn GetMySqlColumnFromSql(DmColumn sqlColumn)
        {
            var mySqlColumn = sqlColumn.Clone();
            mySqlColumn.OriginalTypeName = "mysql";

            // get the sql type (varchar, int, bigint and so on ..)
            var oType = sqlColumn.OriginalTypeName.ToLowerInvariant();
            
            if (oType == "int")
                return mySqlColumn;

            if (oType == "bigint")
                return mySqlColumn;

            if (oType == "smallint")
                return mySqlColumn;

            if (oType == "tinyint")
            {
                // tinyint is unsigned in SQL Server
                // mySql can store a tinyint to 255 if set as unsigned
                mySqlColumn.IsUnsigned = true;
                return mySqlColumn;
            }


            return mySqlColumn;


        }

        
        public override bool CanConvertFrom(DbConverterType type)
        {
            switch (type)
            {
                case DbConverterType.SqlServer:
                    return true;
                case DbConverterType.MySql:
                case DbConverterType.Sqlite:
                default:
                    return false;
            }
        }

        public override DmColumn ConvertFrom(DbConverterType type, DmColumn dmColumn)
        {
            switch (type)
            {
                case DbConverterType.SqlServer:
                    return GetMySqlColumnFromSql(dmColumn);
                case DbConverterType.MySql:
                    throw new ArgumentException("Don't use a converter when both Server and Client are the same");
                case DbConverterType.Sqlite:
                    throw new NotImplementedException("Can't get a MySql column from a Sqlite column");
            }

            return null;
        }

    }
}
