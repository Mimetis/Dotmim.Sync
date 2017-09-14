using Dotmim.Sync.Data;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace Dotmim.Sync.MySql
{
    public class MySqlMetadata
    {

        public static bool SupportScale(string typename)
        {
            string lowerType = typename.ToLowerInvariant();
            switch (lowerType)
            {
                case "numeric":
                case "decimal":
                case "dec":
                case "real": return true;
            }
            return false;
        }

        public static bool IsTextType(string typename)
        {
            string lowerType = typename.ToLowerInvariant();
            switch (lowerType)
            {
                case "varchar":
                case "char":
                case "text":
                case "longtext":
                case "tinytext":
                case "mediumtext":
                case "nchar":
                case "nvarchar":
                case "enum":
                case "set":
                    return true;
            }
            return false;
        }

        public static bool IsNumericType(string typename)
        {
            string lowerType = typename.ToLowerInvariant();
            switch (lowerType)
            {
                case "int":
                case "integer":
                case "numeric":
                case "decimal":
                case "dec":
                case "fixed":
                case "tinyint":
                case "mediumint":
                case "bigint":
                case "real":
                case "double":
                case "float":
                case "serial":
                case "smallint": return true;
            }
            return false;
        }


        private static string GetPrecision(string type, DmColumn column)
        {
            var precision = column.Precision;
            var scale = column.Scale;
            if (IsNumericType(column.OrginalDbType) && !column.PrecisionSpecified)
            {
                precision = 10;
                scale = 0;
            }
            if (!SupportScale(type) || !column.ScaleSpecified)
                return String.Format("({0})", precision);

            return String.Format("({0},{1})", precision, scale);
        }

        /// <summary>
        /// Convert a MySqlDbType to managed type
        /// </summary>
        public static Type MySqlDbTypeToType(NpgsqlDbType npgsqlDbType)
        {
            switch (npgsqlDbType)
            {
                case NpgsqlDbType.Bigint:
                    return typeof(long);
                case NpgsqlDbType.Double:
                    return typeof(Double);
                case NpgsqlDbType.Integer:
                    return typeof(Int32);
                case NpgsqlDbType.Numeric:
                    return typeof(decimal);
                case NpgsqlDbType.Real:
                    return typeof(float);
                case NpgsqlDbType.Smallint:
                    return typeof(Int16);
                case NpgsqlDbType.Boolean:
                    return typeof(Boolean);
                case NpgsqlDbType.Enum:
                    return typeof(string);
                case NpgsqlDbType.Money:
                    return typeof(double);
                case NpgsqlDbType.Char:
                    return typeof(char);
                case NpgsqlDbType.Text:
                case NpgsqlDbType.Varchar:
                case NpgsqlDbType.Name:
                case NpgsqlDbType.Citext:
                    return typeof(string);
                case NpgsqlDbType.Date:
                    return typeof(DateTime);
                case NpgsqlDbType.Time:
                    return typeof(TimeSpan);
                case NpgsqlDbType.Timestamp:
                    return typeof(long);
                case NpgsqlDbType.TimestampTZ:
                    return typeof(long);
                case NpgsqlDbType.TimeTZ:
                    return typeof(long);
                case NpgsqlDbType.Inet:
                case NpgsqlDbType.Cidr:
                case NpgsqlDbType.MacAddr:
                    return typeof(string);
                case NpgsqlDbType.Bit:
                    return typeof(bool);
                case NpgsqlDbType.Varbit:
                    return typeof(bool);
                case NpgsqlDbType.TsVector:
                case NpgsqlDbType.TsQuery:
                case NpgsqlDbType.Uuid:
                    return typeof(Guid);
                case NpgsqlDbType.Xml:
                    return typeof(String);
                case NpgsqlDbType.Json:
                    return typeof(String);
                case NpgsqlDbType.Jsonb:
                    return typeof(byte[]);
                case NpgsqlDbType.InternalChar:
                case NpgsqlDbType.Bytea:
                case NpgsqlDbType.Interval:
                case NpgsqlDbType.Box:
                case NpgsqlDbType.Circle:
                case NpgsqlDbType.Line:
                case NpgsqlDbType.LSeg:
                case NpgsqlDbType.Path:
                case NpgsqlDbType.Point:
                case NpgsqlDbType.Polygon:
                case NpgsqlDbType.Hstore:
                case NpgsqlDbType.Array:
                case NpgsqlDbType.Composite:
                case NpgsqlDbType.Range:
                case NpgsqlDbType.Refcursor:
                case NpgsqlDbType.Oidvector:
                case NpgsqlDbType.Int2Vector:
                case NpgsqlDbType.Oid:
                case NpgsqlDbType.Xid:
                case NpgsqlDbType.Cid:
                case NpgsqlDbType.Regtype:
                case NpgsqlDbType.Tid:
                case NpgsqlDbType.Unknown:
                case NpgsqlDbType.Geometry:
                    break;
            }
            throw new Exception("Unhandled type encountered");
        }

        /// <summary>
        /// Convert a string value from mysql information schema query to a MySqlDbType value
        /// </summary>
        public static NpgsqlDbType NameToMySqlDbType(string typeName)
        {

            throw new Exception("Unhandled type encountered");
        }

        /// <summary>
        /// Convert a MySQLDbType to a string database compatible version
        /// </summary>
        public static string MySqlDbTypeToString(NpgsqlDbType sqlDbType)
        {
            throw new Exception("Unhandled type encountered");
        }


        /// <summary>
        /// Get a type precision string
        /// </summary>
        public static string GetStringPrecisionFromDmColumn(DmColumn column)
        {
            if (!String.IsNullOrEmpty(column.OrginalDbType) && column.Table.OriginalProvider == "MySql")
            {
                var precision = column.Precision;
                var scale = column.Scale;
                if (IsNumericType(column.OrginalDbType) && !column.PrecisionSpecified)
                {
                    precision = 10;
                    scale = 0;
                }
                if (!SupportScale(column.OrginalDbType) || !column.ScaleSpecified)
                    return String.Format("({0})", precision);

                return String.Format("({0},{1})", precision, scale);

            }

            switch (column.DbType)
            {
                case DbType.AnsiString:
                case DbType.AnsiStringFixedLength:
                case DbType.Binary:
                case DbType.String:
                case DbType.StringFixedLength:
                    if (column.MaxLength > 0)
                        return $"({column.MaxLength})";
                    else
                        return string.Empty;
                case DbType.Decimal:
                case DbType.Double:
                case DbType.Single:
                case DbType.VarNumeric:
                    if (!column.PrecisionSpecified || !column.ScaleSpecified)
                        break;

                    return $"({ column.Precision}, {column.Scale})";
            }

            return string.Empty;
        }

        /// <summary>
        /// Get String representation for a DmColumn. If provide is MySql, returns original db type
        /// </summary>
        public static string GetStringTypeFromDmColumn(DmColumn column)
        {
            if (!String.IsNullOrEmpty(column.OrginalDbType) && column.Table?.OriginalProvider == "MySql")
                return column.OrginalDbType;

            string mySqlType = string.Empty;
            switch (column.DbType)
            {
                case DbType.Binary:
                    mySqlType = "BINARY";
                    break;
                case DbType.Boolean:
                case DbType.Byte:
                case DbType.SByte:
                    mySqlType = "TINYINT";
                    break;
                case DbType.Time:
                    mySqlType = "TIME";
                    break;
                case DbType.Date:
                    mySqlType = "DATE";
                    break;
                case DbType.DateTime:
                case DbType.DateTime2:
                case DbType.DateTimeOffset:
                    mySqlType = "DATETIME";
                    break;
                case DbType.Currency:
                case DbType.Decimal:
                case DbType.Double:
                case DbType.Single:
                case DbType.VarNumeric:
                    mySqlType = "DECIMAL";
                    break;
                case DbType.Int16:
                case DbType.UInt16:
                    mySqlType = "SMALLINT";
                    break;
                case DbType.Int32:
                case DbType.UInt32:
                    mySqlType = "INT";
                    break;
                case DbType.Int64:
                case DbType.UInt64:
                    mySqlType = "BIGINT";
                    break;
                case DbType.String:
                case DbType.StringFixedLength:
                case DbType.Xml:
                case DbType.AnsiString:
                case DbType.AnsiStringFixedLength:
                case DbType.Guid:
                    if (column.MaxLength <= 0)
                        mySqlType = "LONGTEXT";
                    else
                        mySqlType = "VARCHAR";
                    break;
                case DbType.Object:
                    mySqlType = "BLOB";
                    break;
            }

            if (string.IsNullOrEmpty(mySqlType))
                throw new Exception($"sqltype not valid for the column {column.ColumnName}");

            return mySqlType;


        }


    }
}
