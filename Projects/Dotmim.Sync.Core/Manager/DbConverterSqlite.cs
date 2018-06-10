using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace Dotmim.Sync.Manager
{
    public class DbConverterSqlite : DbConverter
    {
        public bool IsValid(DmColumn columnDefinition)
        {
            switch (columnDefinition.OriginalTypeName.ToLowerInvariant())
            {
                case "integer":
                case "numeric":
                case "blob":
                case "datetime":
                case "text":
                    return true;
            }
            return false;
        }

        public DbConverterSqlite()
        {

        }

        /// <summary>
        /// Convert a Sql Server column to Sqlite
        /// </summary>
        private static DmColumn GetSqliteColumnFromSqlServer(DmColumn mySqlColumn)
        {
            // get the mysql type (int, text, longtext, decimal and so on ..)
            var oType = mySqlColumn.OriginalTypeName.ToLowerInvariant();

            switch (oType)
            {
                case "uniqueidentifier":
                case "binary":
                case "sql_variant":
                case "image":
                case "varbinary":
                    return ToSqliteBlob(mySqlColumn);
                case "date":
                case "datetime":
                case "datetime2":
                case "smalldatetime":
                    return ToSqliteDate(mySqlColumn);
                case "numeric":
                case "float":
                case "decimal":
                case "real":
                case "smallmoney":
                case "money":
                    return ToSqliteNumeric(mySqlColumn);
                case "int":
                case "smallint":
                case "bigint":
                case "tinyint":
                case "bit":
                case "timestamp":
                    return ToSqliteInt(mySqlColumn);
                case "datetimeoffset":
                case "time":
                case "char":
                case "nchar":
                case "text":
                case "ntext":
                case "varchar":
                case "nvarchar":
                case "xml":
                    return ToSqliteText(mySqlColumn);
                default:
                    throw new NotSupportedException($"this type {oType} is not supported to convert from SqlServer to Sqlite...");
            }

        }
      
        /// <summary>
        /// Convert a MySql column to Sqlite
        /// </summary>
        private static DmColumn GetSqliteColumnFromMySql(DmColumn mySqlColumn)
        {
            // get the mysql type (int, text, longtext, decimal and so on ..)
            var oType = mySqlColumn.OriginalTypeName.ToLowerInvariant();

            switch (oType)
            {
                case "blob":
                case "mediumblob":
                case "longblob":
                case "binary":
                case "varbinary":
                case "tinyblob":
                    return ToSqliteBlob(mySqlColumn);
                case "date":
                case "datetime":
                    return ToSqliteDate(mySqlColumn);
                case "double":
                case "float":
                case "decimal":
                    return ToSqliteNumeric(mySqlColumn);
                case "int":
                case "integer":
                case "mediumint":
                case "bigint":
                case "timestamp":
                case "tinyint":
                case "year":
                case "bit":
                    return ToSqliteInt(mySqlColumn);
                case "char":
                case "nchar":
                case "longtext":
                case "mediumtext":
                case "varchar":
                case "nvarchar":
                case "text":
                case "tinytext":
                    return ToSqliteText(mySqlColumn);
                default:
                    throw new NotSupportedException($"this type {oType} is not supported to convert from MySql to Sqlite...");
            }

        }

        private static DmColumn ToSqliteDate(DmColumn mySqlColumn)
        {
            var column = mySqlColumn.Clone();

            column.MaxLength = -1;
            column.Scale = 0;
            column.Precision = 0;
            column.DbType = DbType.DateTime;
            column.OriginalDbType = "DbType.DateTime";
            column.OriginalTypeName = "datetime";
            return column;
        }
        private static DmColumn ToSqliteText(DmColumn mySqlColumn)
        {
            var column = mySqlColumn.Clone();

            // No need to precise max length for Sqlite
            column.MaxLength = -1;
            column.Scale = 0;
            column.Precision = 0;
            column.DbType = DbType.String;
            column.OriginalDbType = "DbType.String";
            column.OriginalTypeName = "text";
            return column;
        }

        private static DmColumn ToSqliteInt(DmColumn mySqlColumn)
        {
            var column = mySqlColumn.Clone();

            column.MaxLength = -1;
            column.Scale = 0;
            column.Precision = 0;
            column.DbType = DbType.Int64;
            column.OriginalDbType = "DbType.Int64";
            column.OriginalTypeName = "integer";
            return column;
        }

        private static DmColumn ToSqliteBlob(DmColumn mySqlColumn)
        {
            var column = mySqlColumn.Clone();

            column.MaxLength = -1;
            column.Scale = 0;
            column.Precision = 0;
            column.DbType = DbType.Binary;
            column.OriginalDbType = "DbType.Binary";
            column.OriginalTypeName = "binary";
            return column;
        }

       private static DmColumn ToSqliteNumeric(DmColumn mySqlColumn)
        {
            var column = mySqlColumn.Clone();

            column.MaxLength = -1;
            // no need to specify scale and precision for Sqlite
            column.Scale = 0;
            column.Precision = 0;
            column.DbType = DbType.VarNumeric;
            column.OriginalDbType = "DbType.VarNumeric";
            column.OriginalTypeName = "numeric";

            return column;
        }

        public override bool CanConvertFrom(DbConverterType type)
        {
            switch (type)
            {
                case DbConverterType.SqlServer:
                case DbConverterType.MySql:
                    return true;
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
                    return GetSqliteColumnFromSqlServer(dmColumn);
                case DbConverterType.MySql:
                    return GetSqliteColumnFromMySql(dmColumn);
                case DbConverterType.Sqlite:
                    throw new ArgumentException("Don't use a converter when both Server and Client are the same");
            }

            return null;
        }

        public override bool IsValid(string type)
        {
            throw new NotImplementedException();
        }
    }
}