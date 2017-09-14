using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Manager;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace Dotmim.Sync.MySql
{
    //public class MySqlMetadata
    //{

    //    public static bool SupportScale(string typename)
    //    {
    //        string lowerType = typename.ToLowerInvariant();
    //        switch (lowerType)
    //        {
    //            case "numeric":
    //            case "decimal":
    //            case "dec":
    //            case "real": return true;
    //        }
    //        return false;
    //    }

    //    public static bool IsTextType(string typename)
    //    {
    //        string lowerType = typename.ToLowerInvariant();
    //        switch (lowerType)
    //        {
    //            case "varchar":
    //            case "char":
    //            case "text":
    //            case "longtext":
    //            case "tinytext":
    //            case "mediumtext":
    //            case "nchar":
    //            case "nvarchar":
    //            case "enum":
    //            case "set":
    //                return true;
    //        }
    //        return false;
    //    }

    //    public static bool IsNumericType(string typename)
    //    {
    //        string lowerType = typename.ToLowerInvariant();
    //        switch (lowerType)
    //        {
    //            case "int":
    //            case "integer":
    //            case "numeric":
    //            case "decimal":
    //            case "dec":
    //            case "fixed":
    //            case "tinyint":
    //            case "mediumint":
    //            case "bigint":
    //            case "real":
    //            case "double":
    //            case "float":
    //            case "serial":
    //            case "smallint": return true;
    //        }
    //        return false;
    //    }


    //    private static string GetPrecision(string type, DmColumn column)
    //    {
    //        var precision = column.Precision;
    //        var scale = column.Scale;
    //        if (IsNumericType(column.OrginalDbType) && !column.PrecisionSpecified)
    //        {
    //            precision = 10;
    //            scale = 0;
    //        }
    //        if (!SupportScale(type) || !column.ScaleSpecified)
    //            return String.Format("({0})", precision);

    //        return String.Format("({0},{1})", precision, scale);
    //    }

    //    /// <summary>
    //    /// Convert a MySqlDbType to managed type
    //    /// </summary>
    //    public static Type MySqlDbTypeToType(MySqlDbType sqlDbType)
    //    {
    //        switch (sqlDbType)
    //        {
    //            case MySqlDbType.Decimal:
    //            case MySqlDbType.NewDecimal:
    //                return typeof(decimal);
    //            case MySqlDbType.Byte:
    //                return typeof(sbyte);
    //            case MySqlDbType.UByte:
    //                return typeof(byte);
    //            case MySqlDbType.Int16:
    //            case MySqlDbType.Year:
    //                return typeof(short);
    //            case MySqlDbType.Int24:
    //            case MySqlDbType.Int32:
    //                return typeof(Int32);
    //            case MySqlDbType.UInt16:
    //                return typeof(ushort);
    //            case MySqlDbType.Int64:
    //                return typeof(long);
    //            case MySqlDbType.UInt24:
    //            case MySqlDbType.UInt32:
    //                return typeof(UInt32);
    //            case MySqlDbType.Bit:
    //            case MySqlDbType.UInt64:
    //                return typeof(ulong);
    //            case MySqlDbType.Float:
    //                return typeof(float);
    //            case MySqlDbType.Double:
    //                return typeof(double);
    //            case MySqlDbType.Time:
    //                return typeof(TimeSpan);
    //            case MySqlDbType.Date:
    //            case MySqlDbType.DateTime:
    //            case MySqlDbType.Newdate:
    //                return typeof(DateTime);
    //            case MySqlDbType.Enum:
    //            case MySqlDbType.VarString:
    //            case MySqlDbType.JSON:
    //            case MySqlDbType.VarChar:
    //            case MySqlDbType.String:
    //            case MySqlDbType.TinyText:
    //            case MySqlDbType.MediumText:
    //            case MySqlDbType.LongText:
    //            case MySqlDbType.Text:
    //            case MySqlDbType.Set:
    //                return typeof(string);
    //            case MySqlDbType.Guid:
    //                return typeof(Guid);
    //            case MySqlDbType.Timestamp:
    //            case MySqlDbType.TinyBlob:
    //            case MySqlDbType.MediumBlob:
    //            case MySqlDbType.LongBlob:
    //            case MySqlDbType.Blob:
    //            case MySqlDbType.Geometry:
    //            case MySqlDbType.VarBinary:
    //            case MySqlDbType.Binary:
    //                return typeof(byte[]);
    //        }
    //        throw new Exception("Unhandled type encountered");
    //    }

    //    /// <summary>
    //    /// Convert a string value from mysql information schema query to a MySqlDbType value
    //    /// </summary>
    //    public static MySqlDbType NameToMySqlDbType(string typeName, bool unsigned = false)
    //    {
    //        switch (typeName.ToUpperInvariant())
    //        {
    //            case "CHAR":
    //                return MySqlDbType.String;
    //            case "VARCHAR":
    //                return MySqlDbType.VarChar;
    //            case "DATE":
    //                return MySqlDbType.Date;
    //            case "DATETIME":
    //                return MySqlDbType.DateTime;
    //            case "NUMERIC":
    //            case "DECIMAL":
    //            case "DEC":
    //            case "FIXED":
    //                return MySqlDbType.Decimal;
    //            case "YEAR":
    //                return MySqlDbType.Year;
    //            case "TIME":
    //                return MySqlDbType.Time;
    //            case "TIMESTAMP":
    //                return MySqlDbType.Timestamp;
    //            case "SET":
    //                return MySqlDbType.Set;
    //            case "ENUM":
    //                return MySqlDbType.Enum;
    //            case "BIT":
    //                return MySqlDbType.Bit;
    //            case "TINYINT":
    //                return unsigned ? MySqlDbType.UByte : MySqlDbType.Byte;
    //            case "BOOL":
    //            case "BOOLEAN":
    //                return MySqlDbType.Byte;
    //            case "SMALLINT":
    //                return unsigned ? MySqlDbType.UInt16 : MySqlDbType.Int16;
    //            case "MEDIUMINT":
    //                return unsigned ? MySqlDbType.UInt24 : MySqlDbType.Int24;
    //            case "INT":
    //            case "INTEGER":
    //                return unsigned ? MySqlDbType.UInt32 : MySqlDbType.Int32;
    //            case "SERIAL":
    //                return MySqlDbType.UInt64;
    //            case "BIGINT":
    //                return unsigned ? MySqlDbType.UInt64 : MySqlDbType.Int64;
    //            case "FLOAT":
    //                return MySqlDbType.Float;
    //            case "DOUBLE":
    //                return MySqlDbType.Double;
    //            case "REAL":
    //                return MySqlDbType.Float;
    //            case "TEXT":
    //                return MySqlDbType.Text;
    //            case "BLOB":
    //                return MySqlDbType.Blob;
    //            case "LONGBLOB":
    //                return MySqlDbType.LongBlob;
    //            case "LONGTEXT":
    //                return MySqlDbType.LongText;
    //            case "MEDIUMBLOB":
    //                return MySqlDbType.MediumBlob;
    //            case "MEDIUMTEXT":
    //                return MySqlDbType.MediumText;
    //            case "TINYBLOB":
    //                return MySqlDbType.TinyBlob;
    //            case "TINYTEXT":
    //                return MySqlDbType.TinyText;
    //            case "BINARY":
    //                return MySqlDbType.Binary;
    //            case "VARBINARY":
    //                return MySqlDbType.VarBinary;
    //        }
    //        throw new Exception("Unhandled type encountered");
    //    }

    //    /// <summary>
    //    /// Convert a MySQLDbType to a string database compatible version
    //    /// </summary>
    //    public static string MySqlDbTypeToString(MySqlDbType sqlDbType)
    //    {
    //        switch (sqlDbType)
    //        {
    //            case MySqlDbType.Decimal:
    //            case MySqlDbType.NewDecimal:
    //                return "DECIMAL";
    //            case MySqlDbType.Byte:
    //            case MySqlDbType.UByte:
    //                return "TINYINT";
    //            case MySqlDbType.Int16:
    //            case MySqlDbType.Year:
    //            case MySqlDbType.UInt16:
    //                return "SMALLINT";
    //            case MySqlDbType.Int24:
    //            case MySqlDbType.UInt24:
    //                return "MEDIUMINT";
    //            case MySqlDbType.Int32:
    //            case MySqlDbType.UInt32:
    //                return "INT";
    //            case MySqlDbType.Int64:
    //            case MySqlDbType.UInt64:
    //                return "BIGINT";
    //            case MySqlDbType.Bit:
    //                return "BIT";
    //            case MySqlDbType.Float:
    //                return "FLOAT";
    //            case MySqlDbType.Double:
    //                return "DOUBLE";
    //            case MySqlDbType.Time:
    //                return "TIME";
    //            case MySqlDbType.Date:
    //                return "DATE";
    //            case MySqlDbType.DateTime:
    //                return "DATETIME";
    //            case MySqlDbType.Newdate:
    //                return "NEWDATE";
    //            case MySqlDbType.Enum:
    //                return "ENUM";
    //            case MySqlDbType.TinyText:
    //                return "TINYTEXT";
    //            case MySqlDbType.MediumText:
    //                return "MEDIUMTEXT";
    //            case MySqlDbType.LongText:
    //                return "LONGTEXT";
    //            case MySqlDbType.Text:
    //                return "TEXT";
    //            case MySqlDbType.JSON:
    //            case MySqlDbType.VarChar:
    //            case MySqlDbType.VarString:
    //            case MySqlDbType.String:
    //            case MySqlDbType.Guid:
    //                return "VARCHAR";
    //            case MySqlDbType.Set:
    //                return "SET";
    //            case MySqlDbType.Timestamp:
    //                return "TIMESTAMP";
    //            case MySqlDbType.TinyBlob:
    //                return "TINYBLOB";
    //            case MySqlDbType.MediumBlob:
    //                return "MEDIUMBLOB";
    //            case MySqlDbType.LongBlob:
    //                return "LONGBLOB";
    //            case MySqlDbType.Blob:
    //                return "BLOB";
    //            case MySqlDbType.VarBinary:
    //                return "VARBINARY";
    //            case MySqlDbType.Binary:
    //                return "BINARY";
    //            case MySqlDbType.Geometry:
    //                return "GEOMETRY";
    //        }
    //        throw new Exception("Unhandled type encountered");
    //    }


    //    /// <summary>
    //    /// Get a type precision string
    //    /// </summary>
    //    public static string GetStringPrecisionFromDmColumn(DmColumn column)
    //    {
    //        if (!String.IsNullOrEmpty(column.OrginalDbType) && column.Table.OriginalProvider == "MySql")
    //        {
    //            var precision = column.Precision;
    //            var scale = column.Scale;
    //            if (IsNumericType(column.OrginalDbType) && !column.PrecisionSpecified)
    //            {
    //                precision = 10;
    //                scale = 0;
    //            }
    //            if (!SupportScale(column.OrginalDbType) || !column.ScaleSpecified)
    //                return String.Format("({0})", precision);

    //            return String.Format("({0},{1})", precision, scale);

    //        }

    //        switch (column.DbType)
    //        {
    //            case DbType.AnsiString:
    //            case DbType.AnsiStringFixedLength:
    //            case DbType.Binary:
    //            case DbType.String:
    //            case DbType.StringFixedLength:
    //                if (column.MaxLength > 0)
    //                    return $"({column.MaxLength})";
    //                else
    //                    return string.Empty;
    //            case DbType.Decimal:
    //            case DbType.Double:
    //            case DbType.Single:
    //            case DbType.VarNumeric:
    //                if (!column.PrecisionSpecified || !column.ScaleSpecified)
    //                    break;

    //                return $"({ column.Precision}, {column.Scale})";
    //            case DbType.Guid:
    //                return "(36)";
    //        }

    //        return string.Empty;
    //    }




    //    public static (byte precision, byte scale)  GetPrecisionFromDmColumn(DmColumn column)
    //    {
    //        if (!String.IsNullOrEmpty(column.OrginalDbType) && column.Table.OriginalProvider == "MySql")
    //        {
    //            var precision = column.Precision;
    //            var scale = column.Scale;
    //            if (IsNumericType(column.OrginalDbType) && !column.PrecisionSpecified)
    //            {
    //                precision = 10;
    //                scale = 0;
    //            }
    //            if (!SupportScale(column.OrginalDbType) || !column.ScaleSpecified)
    //                return (precision, 0);

    //            return (precision, scale);

    //        }

    //        switch (column.DbType)
    //        {
    //            case DbType.Decimal:
    //            case DbType.Double:
    //            case DbType.Single:
    //            case DbType.VarNumeric:
    //                if (!column.PrecisionSpecified || !column.ScaleSpecified)
    //                    break;
    //                return (column.Precision, column.Scale);
    //        }

    //        return (0,0);
    //    }



    //    /// <summary>
    //    /// Get String representation for a DmColumn. If provide is MySql, returns original db type
    //    /// </summary>
    //    public static string GetStringTypeFromDmColumn(DmColumn column)
    //    {
    //        if (!String.IsNullOrEmpty(column.OrginalDbType) && column.Table?.OriginalProvider == "MySql")
    //            return column.OrginalDbType;

    //        return GetStringTypeFromDbType(column.DbType, Convert.ToInt32(column.MaxLength));
    //    }

    //    public static string GetStringTypeFromDbType(DbType dbType, int maxLength= 0)
    //    {
    //        string mySqlType = string.Empty;
    //        switch (dbType)
    //        {
    //            case DbType.Binary:
    //                mySqlType = "BINARY";
    //                break;
    //            case DbType.Boolean:
    //            case DbType.Byte:
    //            case DbType.SByte:
    //                mySqlType = "TINYINT";
    //                break;
    //            case DbType.Time:
    //                mySqlType = "TIME";
    //                break;
    //            case DbType.Date:
    //                mySqlType = "DATE";
    //                break;
    //            case DbType.DateTime:
    //            case DbType.DateTime2:
    //            case DbType.DateTimeOffset:
    //                mySqlType = "DATETIME";
    //                break;
    //            case DbType.Currency:
    //            case DbType.Decimal:
    //            case DbType.Double:
    //            case DbType.Single:
    //            case DbType.VarNumeric:
    //                mySqlType = "DECIMAL";
    //                break;
    //            case DbType.Int16:
    //            case DbType.UInt16:
    //                mySqlType = "SMALLINT";
    //                break;
    //            case DbType.Int32:
    //            case DbType.UInt32:
    //                mySqlType = "INT";
    //                break;
    //            case DbType.Int64:
    //            case DbType.UInt64:
    //                mySqlType = "BIGINT";
    //                break;
    //            case DbType.String:
    //            case DbType.StringFixedLength:
    //            case DbType.Xml:
    //            case DbType.AnsiString:
    //            case DbType.AnsiStringFixedLength:
    //                if (maxLength <= 0)
    //                    mySqlType = "LONGTEXT";
    //                else
    //                    mySqlType = "VARCHAR";
    //                break;
    //            case DbType.Guid:
    //                mySqlType = "VARCHAR";
    //                break;
    //            case DbType.Object:
    //                mySqlType = "BLOB";
    //                break;
    //        }

    //        if (string.IsNullOrEmpty(mySqlType))
    //            throw new Exception($"sqltype not valid");

    //        return mySqlType;
    //    }


    //}


    public class MySqlMetadata : DbMetadata
    {
        public override object GetOwnerDbTypeFromDbType(DbType dbType)
        {
            throw new NotImplementedException();
        }

        public override string GetPrecisionStringFromDbType(DbType dbType, int maxLength, byte precision, byte scale)
        {
            throw new NotImplementedException();
        }

        public override string GetPrecisionStringFromOwnerDbType(object dbType, int maxLength, byte precision, byte scale)
        {
            throw new NotImplementedException();
        }

        public override string GetStringFromDbType(DbType dbType)
        {
            throw new NotImplementedException();
        }

        public override string GetStringFromOwnerDbType(object ownerType)
        {
            throw new NotImplementedException();
        }

        public override bool IsNumericType(string typeName)
        {
            throw new NotImplementedException();
        }

        public override bool IsTextType(string typeName)
        {
            throw new NotImplementedException();
        }

        public override bool IsValid(DbColumnDefinition columnDefinition)
        {
            throw new NotImplementedException();
        }

        public override bool SupportScale(string typeName)
        {
            throw new NotImplementedException();
        }

        public override DbType ValidateDbType(string typeName, bool isUnsigned, bool isUnicode)
        {
            throw new NotImplementedException();
        }

        public override bool ValidateIsReadonly(DbColumnDefinition columnDefinition)
        {
            throw new NotImplementedException();
        }

        public override int ValidateMaxLength(string typeName, bool isUnsigned, bool isUnicode, long maxLength)
        {
            throw new NotImplementedException();
        }

        public override object ValidateOwnerDbType(string typeName, bool isUnsigned, bool isUnicode)
        {
            throw new NotImplementedException();
        }

        public override byte ValidatePrecision(DbColumnDefinition columnDefinition)
        {
            throw new NotImplementedException();
        }

        public override (byte precision, byte scale) ValidatePrecisionAndScale(DbColumnDefinition columnDefinition)
        {
            throw new NotImplementedException();
        }

        public override Type ValidateType(object ownerType)
        {
            throw new NotImplementedException();
        }
    }
}
