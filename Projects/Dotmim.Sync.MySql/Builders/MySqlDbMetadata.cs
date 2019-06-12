using Dotmim.Sync.Manager;
using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using MySql.Data.MySqlClient;
using Dotmim.Sync.Data;

namespace Dotmim.Sync.MySql.Builders
{
    public class MySqlDbMetadata : DbMetadata
    {
        public override int GetMaxLengthFromDbType(DbType dbType, int maxLength)
        {
            var typeName = GetStringFromDbType(dbType);
            if (IsTextType(typeName))
            {
                string lowerType = typeName.ToLowerInvariant();
                switch (lowerType)
                {
                    case "varchar":
                    case "char":
                    case "text":
                    case "nchar":
                    case "nvarchar":
                    case "enum":
                    case "set":
                        if (maxLength > 0)
                            return maxLength;
                        else
                            return 0;
                }
                return 0;
            }
            return 0;
        }

        public override int GetMaxLengthFromOwnerDbType(object dbType, int maxLength)
        {
            var typeName = GetStringFromOwnerDbType(dbType);
            if (IsTextType(typeName))
            {
                string lowerType = typeName.ToLowerInvariant();
                switch (lowerType)
                {
                    case "MEDIUMTEXT":
                    case "LONGTEXT":
                    case "TINYBLOB":
                    case "MEDIUMBLOB":
                    case "LONGBLOB":
                    case "BLOB":
                    case "JSON":
                    case "TINYTEXT":
                        return 0;
                    case "TEXT":
                    case "NCHAR":
                    case "NVARCHAR":
                    case "VARCHAR":
                    case "CHAR":
                    case "ENUM":
                    case "SET":
                        if (maxLength > 0)
                            return maxLength;
                        else
                            return 0;
                }
                return 0;
            }
            return 0;
        }

        public override object GetOwnerDbTypeFromDbType(DbType dbType)
        {
            switch (dbType)
            {
                case DbType.AnsiString:
                case DbType.Xml:
                case DbType.String:
                    return MySqlDbType.LongText;
                case DbType.StringFixedLength:
                case DbType.AnsiStringFixedLength:
                    return MySqlDbType.VarChar;
                case DbType.Binary:
                    return MySqlDbType.LongBlob;
                case DbType.Boolean:
                    return MySqlDbType.Bit;
                case DbType.Byte:
                    return MySqlDbType.UByte;
                case DbType.Currency:
                    return MySqlDbType.Decimal;
                case DbType.Date:
                    return MySqlDbType.Date;
                case DbType.DateTime:
                case DbType.DateTime2:
                case DbType.DateTimeOffset:
                    return MySqlDbType.DateTime;
                case DbType.Decimal:
                    return MySqlDbType.Decimal;
                case DbType.Double:
                    return MySqlDbType.Double;
                case DbType.Guid:
                    return MySqlDbType.Guid;
                case DbType.Int16:
                    return MySqlDbType.Int16;
                case DbType.Int32:
                    return MySqlDbType.Int32;
                case DbType.Int64:
                    return MySqlDbType.Int64;
                case DbType.Object:
                    return MySqlDbType.LongBlob;
                case DbType.SByte:
                    return MySqlDbType.Byte;
                case DbType.Single:
                    return MySqlDbType.Float;
                case DbType.Time:
                    return MySqlDbType.Time;
                case DbType.UInt16:
                    return MySqlDbType.UInt16;
                case DbType.UInt32:
                    return MySqlDbType.UInt32;
                case DbType.UInt64:
                    return MySqlDbType.UInt64;
                case DbType.VarNumeric:
                    return MySqlDbType.Decimal;
            }
            throw new Exception($"this type {dbType} is not supported");
        }

        public override (byte precision, byte scale) GetPrecisionFromDbType(DbType dbType, byte precision, byte scale)
        {
            var typeName = GetStringFromDbType(dbType);
            if (IsNumericType(typeName) && precision == 0)
            {
                precision = 10;
                scale = 0;
            }
            if (!SupportScale(typeName) || scale == 0)
                return (0, 0);

            return (precision, scale);
        }

        public override (byte precision, byte scale) GetPrecisionFromOwnerDbType(object dbType, byte precision, byte scale)
        {
            var typeName = GetStringFromOwnerDbType(dbType);
            if (IsNumericType(typeName) && precision == 0)
            {
                precision = 10;
                scale = 0;
            }
            if (!SupportScale(typeName) || scale == 0)
                return (0, 0);

            return (precision, scale);
        }

        public override string GetPrecisionStringFromDbType(DbType dbType, int maxLength, byte precision, byte scale)
        {
            if (dbType == DbType.Guid)
                return "(36)";

            var typeName = GetStringFromDbType(dbType);
            if (IsTextType(typeName))
            {
                string lowerType = typeName.ToUpperInvariant();
                switch (lowerType)
                {

                    case "MEDIUMTEXT":
                    case "LONGTEXT":
                    case "TINYBLOB":
                    case "MEDIUMBLOB":
                    case "LONGBLOB":
                    case "BLOB":
                    case "JSON":
                    case "TINYTEXT":
                        return string.Empty; ;
                    case "TEXT":
                    case "NCHAR":
                    case "NVARCHAR":
                    case "VARCHAR":
                    case "CHAR":
                    case "ENUM":
                    case "SET":
                        if (maxLength > 0)
                            return $"({maxLength})";
                        else
                            return string.Empty;
                }
                return string.Empty;
            }

            if (IsNumericType(typeName) && precision == 0)
            {
                precision = 10;
                scale = 0;
            }
            if (SupportScale(typeName) && scale == 0)
                return String.Format("({0})", precision);

            if (!SupportScale(typeName))
                return string.Empty;

            return String.Format("({0},{1})", precision, scale);
        }

        public override string GetPrecisionStringFromOwnerDbType(object dbType, int maxLength, byte precision, byte scale)
        {
            MySqlDbType mySqlDbType = (MySqlDbType)dbType;

            if (mySqlDbType == MySqlDbType.Guid)
                return "(36)";

            var typeName = GetStringFromOwnerDbType(dbType);

            if (IsTextType(typeName))
            {
                string lowerType = typeName.ToUpperInvariant();
                switch (lowerType)
                {

                    case "MEDIUMTEXT":
                    case "LONGTEXT":
                    case "TINYBLOB":
                    case "MEDIUMBLOB":
                    case "LONGBLOB":
                    case "BLOB":
                    case "JSON":
                    case "TINYTEXT":
                        return string.Empty; ;
                    case "TEXT":
                    case "NCHAR":
                    case "NVARCHAR":
                    case "VARCHAR":
                    case "CHAR":
                    case "ENUM":
                    case "SET":
                        if (maxLength > 0)
                            return $"({maxLength})";
                        else
                            return string.Empty;

                }
                return string.Empty;
            }

            if (IsNumericType(typeName) && precision == 0)
            {
                precision = 10;
                scale = 0;
            }
            if (SupportScale(typeName) && scale == 0)
                return String.Format("({0})", precision);

            if (!SupportScale(typeName))
                return string.Empty;

            return String.Format("({0},{1})", precision, scale);
        }

        public override string GetStringFromDbType(DbType dbType)
        {
            string mySqlType = string.Empty;
            switch (dbType)
            {
                case DbType.Binary:
                    mySqlType = "LONGBLOB";
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
                case DbType.AnsiString:
                case DbType.Xml:
                    mySqlType = "LONGTEXT";
                    break;
                case DbType.StringFixedLength:
                case DbType.AnsiStringFixedLength:
                    mySqlType = "VARCHAR";
                    break;
                case DbType.Guid:
                    mySqlType = "CHAR";
                    break;
                case DbType.Object:
                    mySqlType = "LONGBLOB";
                    break;
            }

            if (string.IsNullOrEmpty(mySqlType))
                throw new Exception($"sqltype not valid");

            return mySqlType;
        }

        public override string GetStringFromOwnerDbType(object ownerType)
        {
            MySqlDbType sqlDbType = (MySqlDbType)ownerType;

            switch (sqlDbType)
            {
                case MySqlDbType.Decimal:
                case MySqlDbType.NewDecimal:
                    return "DECIMAL";
                case MySqlDbType.Byte:
                case MySqlDbType.Bool:
                case MySqlDbType.UByte:
                    return "TINYINT";
                case MySqlDbType.Int16:
                case MySqlDbType.Year:
                case MySqlDbType.UInt16:
                    return "SMALLINT";
                case MySqlDbType.Int24:
                case MySqlDbType.UInt24:
                    return "MEDIUMINT";
                case MySqlDbType.Int32:
                case MySqlDbType.UInt32:
                    return "INT";
                case MySqlDbType.Int64:
                case MySqlDbType.UInt64:
                    return "BIGINT";
                case MySqlDbType.Bit:
                    return "BIT";
                case MySqlDbType.Float:
                    return "FLOAT";
                case MySqlDbType.Double:
                    return "DOUBLE";
                case MySqlDbType.Time:
                    return "TIME";
                case MySqlDbType.Date:
                    return "DATE";
                case MySqlDbType.DateTime:
                    return "DATETIME";
                case MySqlDbType.Newdate:
                    return "NEWDATE";
                case MySqlDbType.Enum:
                    return "ENUM";
                case MySqlDbType.TinyText:
                    return "TINYTEXT";
                case MySqlDbType.MediumText:
                    return "MEDIUMTEXT";
                case MySqlDbType.LongText:
                    return "LONGTEXT";
                case MySqlDbType.Text:
                    return "TEXT";
                case MySqlDbType.JSON:
                case MySqlDbType.VarChar:
                case MySqlDbType.VarString:
                    return "VARCHAR";
                case MySqlDbType.String:
                case MySqlDbType.Guid:
                    return "CHAR";
                case MySqlDbType.Set:
                    return "SET";
                case MySqlDbType.Timestamp:
                    return "TIMESTAMP";
                case MySqlDbType.TinyBlob:
                    return "TINYBLOB";
                case MySqlDbType.MediumBlob:
                    return "MEDIUMBLOB";
                case MySqlDbType.LongBlob:
                    return "LONGBLOB";
                case MySqlDbType.Blob:
                    return "BLOB";
                case MySqlDbType.VarBinary:
                    return "LONGBLOB";
                case MySqlDbType.Binary:
                    return "LONGBLOB";
                case MySqlDbType.Geometry:
                    return "GEOMETRY";
            }
            throw new Exception("Unhandled type encountered");
        }

        public override bool IsNumericType(string typeName)
        {
            string lowerType = typeName.ToLowerInvariant();
            switch (lowerType)
            {
                case "int":
                case "int16":
                case "int24":
                case "int32":
                case "int64":
                case "uint16":
                case "uint24":
                case "uint32":
                case "uint64":
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

        public override bool IsTextType(string typeName)
        {
            string lowerType = typeName.ToLowerInvariant();
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

        public override bool IsValid(DmColumn columnDefinition)
        {
            switch (columnDefinition.OriginalTypeName.ToLowerInvariant())
            {
                case "int":
                case "int16":
                case "int24":
                case "int32":
                case "int64":
                case "uint16":
                case "uint24":
                case "uint32":
                case "uint64":
                case "bit":
                case "integer":
                case "datetime":
                case "date":
                case "newdate":
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
                case "smallint":
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
                case "blob":
                case "longblob":
                case "tinyblob":
                case "mediumblob":
                case "binary":
                case "varbinary":
                case "year":
                case "time":
                case "timestamp":
                    return true;
            }
            return false;
        }

        public override bool SupportScale(string typeName)
        {
            string lowerType = typeName.ToLowerInvariant();
            switch (lowerType)
            {
                case "numeric":
                case "decimal":
                case "dec":
                case "real": return true;
            }
            return false;
        }

        public override DbType ValidateDbType(string typeName, bool isUnsigned, bool isUnicode, long maxLength)
        {
            switch (typeName.ToLowerInvariant())
            {
                case "int":
                case "integer":
                case "mediumint":
                    return isUnsigned ? DbType.UInt32 : DbType.Int32;
                case "int16":
                    return DbType.Int16;
                case "int24":
                case "int32":
                    return DbType.Int32;
                case "int64":
                    return DbType.Int64;
                case "uint16":
                    return DbType.UInt16;
                case "uint24":
                case "uint32":
                    return DbType.UInt32;
                case "uint64":
                    return DbType.UInt64;
                case "bit":
                    return DbType.Boolean;
                case "numeric":
                    return DbType.VarNumeric;
                case "decimal":
                case "dec":
                case "fixed":
                case "real":
                case "double":
                case "float":
                    return DbType.Decimal;
                case "tinyint":
                    return isUnsigned ? DbType.Byte : DbType.SByte;
                case "bigint":
                    return isUnsigned ? DbType.UInt64 : DbType.Int64;
                case "serial":
                    return DbType.UInt64;
                case "smallint":
                    return isUnsigned ? DbType.UInt16 : DbType.Int16;

                case "mediumtext":
                case "longtext":
                case "json":
                case "tinytext":
                        return isUnicode ? DbType.String : DbType.AnsiString;
                case "text":
                case "nchar":
                case "nvarchar":
                case "varchar":
                case "enum":
                case "set":
                    if (isUnicode)
                        return maxLength <= 0 ? DbType.String : DbType.StringFixedLength;
                    else
                        return maxLength <= 0 ? DbType.AnsiString : DbType.AnsiStringFixedLength;
                case "char":
                    if (maxLength == 36)
                        return DbType.Guid;
                    else if (isUnicode)
                        return maxLength <= 0 ? DbType.String : DbType.StringFixedLength;
                    else
                        return maxLength <= 0 ? DbType.AnsiString : DbType.AnsiStringFixedLength;

                case "date":
                    return DbType.Date;
                case "datetime":
                case "newdate":
                    return DbType.DateTime;
                case "blob":
                case "longblob":
                case "tinyblob":
                case "mediumblob":
                case "binary":
                case "varbinary":
                    return DbType.Binary;
                case "year":
                    return DbType.Int32;
                case "time":
                    return DbType.Time;
                case "timestamp":
                    return DbType.UInt64;
            }
            throw new Exception($"this type name {typeName} is not supported");
        }

        public override bool ValidateIsReadonly(DmColumn columnDefinition)
        {
            return columnDefinition.OriginalTypeName.ToLowerInvariant() == "timestamp";
        }

        public override int ValidateMaxLength(string typeName, bool isUnsigned, bool isUnicode, long maxLength)
        {
            Int32 iMaxLength = maxLength > 8000 ? 8000 : Convert.ToInt32(maxLength);
            return iMaxLength;
        }

        public override object ValidateOwnerDbType(string typeName, bool isUnsigned, bool isUnicode, long maxLength)
        {
            switch (typeName.ToUpperInvariant())
            {
                case "CHAR":
                    if (maxLength == 36)
                        return MySqlDbType.Guid;
                    else
                        return MySqlDbType.String;
                case "GUID":
                    return MySqlDbType.Guid;
                case "STRING":
                    return MySqlDbType.String;
                case "VARCHAR":
                    return MySqlDbType.VarChar;
                case "DATE":
                    return MySqlDbType.Date;
                case "DATETIME":
                    return MySqlDbType.DateTime;
                case "NUMERIC":
                case "DECIMAL":
                case "DEC":
                case "FIXED":
                    return MySqlDbType.Decimal;
                case "YEAR":
                    return MySqlDbType.Year;
                case "TIME":
                    return MySqlDbType.Time;
                case "TIMESTAMP":
                    return MySqlDbType.Timestamp;
                case "SET":
                    return MySqlDbType.Set;
                case "ENUM":
                    return MySqlDbType.Enum;
                case "BIT":
                    return MySqlDbType.Bit;
                case "BYTE":
                    return MySqlDbType.Byte;
                case "UBYTE":
                    return MySqlDbType.UByte;
                case "TINYINT":
                    return isUnsigned ? MySqlDbType.UByte : MySqlDbType.Byte;
                case "BOOL":
                case "BOOLEAN":
                    return MySqlDbType.Byte;
                case "SMALLINT":
                    return isUnsigned ? MySqlDbType.UInt16 : MySqlDbType.Int16;
                case "MEDIUMINT":
                    return isUnsigned ? MySqlDbType.UInt24 : MySqlDbType.Int24;
                case "INT":
                case "INTEGER":
                    return isUnsigned ? MySqlDbType.UInt32 : MySqlDbType.Int32;
                case "SERIAL":
                    return MySqlDbType.UInt64;
                case "BIGINT":
                    return isUnsigned ? MySqlDbType.UInt64 : MySqlDbType.Int64;
                case "UINT16":
                    return MySqlDbType.UInt16;
                case "UINT24":
                    return MySqlDbType.UInt24;
                case "UINT32":
                    return MySqlDbType.UInt32;
                case "UINT64":
                    return MySqlDbType.UInt64;
                case "INT16":
                    return MySqlDbType.Int16;
                case "INT24":
                    return MySqlDbType.Int24;
                case "INT32":
                    return MySqlDbType.Int32;
                case "INT64":
                    return MySqlDbType.Int64;
                case "FLOAT":
                    return MySqlDbType.Float;
                case "DOUBLE":
                    return MySqlDbType.Double;
                case "REAL":
                    return MySqlDbType.Float;
                case "TEXT":
                    return MySqlDbType.Text;
                case "BLOB":
                    return MySqlDbType.Blob;
                case "LONGBLOB":
                    return MySqlDbType.LongBlob;
                case "LONGTEXT":
                    return MySqlDbType.LongText;
                case "MEDIUMBLOB":
                    return MySqlDbType.MediumBlob;
                case "MEDIUMTEXT":
                    return MySqlDbType.MediumText;
                case "TINYBLOB":
                    return MySqlDbType.TinyBlob;
                case "TINYTEXT":
                    return MySqlDbType.TinyText;
                case "BINARY":
                    return MySqlDbType.Binary;
                case "VARBINARY":
                    return MySqlDbType.VarBinary;
            }
            throw new Exception("Unhandled type encountered");
        }

        public override byte ValidatePrecision(DmColumn columnDefinition)
        {
            if (IsNumericType(columnDefinition.OriginalTypeName) && columnDefinition.Precision == 0)
                return 10;

            return columnDefinition.Precision;
        }

        public override (byte precision, byte scale) ValidatePrecisionAndScale(DmColumn columnDefinition)
        {
            var precision = columnDefinition.Precision;
            var scale = columnDefinition.Scale;
            if (IsNumericType(columnDefinition.OriginalTypeName) && precision == 0)
            {
                precision = 10;
                scale = 0;
            }

            return (precision, scale);
        }

        public override Type ValidateType(object ownerType)
        {
            MySqlDbType sqlDbType = (MySqlDbType)ownerType;

            switch (sqlDbType)
            {
                case MySqlDbType.Decimal:
                case MySqlDbType.NewDecimal:
                    return typeof(decimal);
                case MySqlDbType.Byte:
                    return typeof(sbyte);
                case MySqlDbType.UByte:
                    return typeof(byte);
                case MySqlDbType.Int16:
                case MySqlDbType.Year:
                    return typeof(short);
                case MySqlDbType.Int24:
                case MySqlDbType.Int32:
                    return typeof(Int32);
                case MySqlDbType.UInt16:
                    return typeof(ushort);
                case MySqlDbType.Int64:
                    return typeof(long);
                case MySqlDbType.UInt24:
                case MySqlDbType.UInt32:
                    return typeof(UInt32);
                case MySqlDbType.Bit:
                case MySqlDbType.UInt64:
                    return typeof(ulong);
                case MySqlDbType.Float:
                    return typeof(float);
                case MySqlDbType.Double:
                    return typeof(double);
                case MySqlDbType.Time:
                    return typeof(TimeSpan);
                case MySqlDbType.Date:
                case MySqlDbType.DateTime:
                case MySqlDbType.Newdate:
                    return typeof(DateTime);
                case MySqlDbType.Enum:
                case MySqlDbType.VarString:
                case MySqlDbType.JSON:
                case MySqlDbType.VarChar:
                case MySqlDbType.String:
                case MySqlDbType.TinyText:
                case MySqlDbType.MediumText:
                case MySqlDbType.LongText:
                case MySqlDbType.Text:
                case MySqlDbType.Set:
                    return typeof(string);
                case MySqlDbType.Guid:
                    return typeof(Guid);
                case MySqlDbType.Timestamp:
                case MySqlDbType.TinyBlob:
                case MySqlDbType.MediumBlob:
                case MySqlDbType.LongBlob:
                case MySqlDbType.Blob:
                case MySqlDbType.Geometry:
                case MySqlDbType.VarBinary:
                case MySqlDbType.Binary:
                    return typeof(byte[]);
            }
            throw new Exception("Unhandled type encountered");
        }

    }
}
