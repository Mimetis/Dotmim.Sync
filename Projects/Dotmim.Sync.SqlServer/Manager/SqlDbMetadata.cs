using Dotmim.Sync.Manager;
using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Linq;
using Dotmim.Sync.Data;

namespace Dotmim.Sync.SqlServer.Manager
{
    public class SqlDbMetadata : DbMetadata
    {

        // Even if precision max can be 38 on SQL Server, prefer go for 28, to not having a truncation
        public const Byte PRECISION_MAX = 28;
        public const Byte SCALE_MAX = 18;

        /// <summary>
        /// Gets the DbType issue from the server type name
        /// </summary>
        public override DbType ValidateDbType(string typeName, bool isUnsigned, bool isUnicode, long maxLength)
        {
            switch (typeName.ToLowerInvariant())
            {
                case "bigint":
                    return DbType.Int64;
                case "binary":
                    return DbType.Binary;
                case "bit":
                    return DbType.Boolean;
                case "char":
                    return DbType.AnsiStringFixedLength;
                case "date":
                    return DbType.Date;
                case "datetime":
                    return DbType.DateTime;
                case "datetime2":
                    return DbType.DateTime2;
                case "datetimeoffset":
                    return DbType.DateTimeOffset;
                case "decimal":
                    return DbType.Decimal;
                case "float":
                    return DbType.Double;
                case "int":
                    return DbType.Int32;
                case "money":
                case "smallmoney":
                    return DbType.Currency;
                case "nchar":
                    return DbType.StringFixedLength;
                case "numeric":
                    return DbType.VarNumeric;
                case "nvarchar":
                    return maxLength <= 0 ? DbType.String : DbType.StringFixedLength;
                case "real":
                    return DbType.Decimal;
                case "smalldatetime":
                    return DbType.DateTime;
                case "smallint":
                    return DbType.Int16;
                case "sql_variant":
                case "variant":
                    return DbType.Object;
                case "time":
                    return DbType.Time;
                case "timestamp":
                    return DbType.Int64;
                case "tinyint":
                    return DbType.Int16;
                case "uniqueidentifier":
                    return DbType.Guid;
                case "varbinary":
                    return DbType.Binary;
                case "varchar":
                    return maxLength <= 0 ? DbType.AnsiString : DbType.AnsiStringFixedLength;
                case "xml":
                    return DbType.String;
            }
            throw new Exception($"this type name {typeName} is not supported");
        }

        /// <summary>
        /// Gets the SqlDbType issued from the server type name
        /// </summary>
        public override object ValidateOwnerDbType(string typeName, bool isUnsigned, bool isUnicode, long maxLength)
        {
            switch (typeName.ToLowerInvariant())
            {
                case "bigint":
                    return SqlDbType.BigInt;
                case "binary":
                    return SqlDbType.Binary;
                case "bit":
                    return SqlDbType.Bit;
                case "char":
                    return SqlDbType.Char;
                case "date":
                    return SqlDbType.Date;
                case "datetime":
                    return SqlDbType.DateTime;
                case "datetime2":
                    return SqlDbType.DateTime2;
                case "datetimeoffset":
                    return SqlDbType.DateTimeOffset;
                case "decimal":
                    return SqlDbType.Decimal;
                case "float":
                    return SqlDbType.Float;
                case "int":
                    return SqlDbType.Int;
                case "money":
                    return SqlDbType.Money;
                case "smallmoney":
                    return SqlDbType.SmallMoney;
                case "nchar":
                    return SqlDbType.NChar;
                case "numeric":
                    return SqlDbType.Decimal;
                case "nvarchar":
                    return SqlDbType.NVarChar;
                case "real":
                    return SqlDbType.Real;
                case "smalldatetime":
                    return SqlDbType.SmallDateTime;
                case "smallint":
                    return SqlDbType.SmallInt;
                case "sql_variant":
                case "variant":
                    return SqlDbType.Variant;
                case "time":
                    return SqlDbType.Time;
                case "timestamp":
                    return SqlDbType.Timestamp;
                case "tinyint":
                    return SqlDbType.TinyInt;
                case "uniqueidentifier":
                    return SqlDbType.UniqueIdentifier;
                case "varbinary":
                    return SqlDbType.VarBinary;
                case "varchar":
                    return SqlDbType.VarChar;
                case "xml":
                    return SqlDbType.Xml;
            }
            throw new Exception($"this type name {typeName} is not supported");
        }

        /// <summary>
        /// Gets the max length autorized
        /// </summary>
        public override int ValidateMaxLength(string typeName, bool isUnsigned, bool isUnicode, long maxLength)
        {
            SqlDbType sqlDbType = (SqlDbType)ValidateOwnerDbType(typeName, isUnsigned, isUnicode, maxLength);

            Int32 iMaxLength = maxLength > 8000 ? 8000 : Convert.ToInt32(maxLength);

            // special length for nchar and nvarchar
            if ((sqlDbType == SqlDbType.NChar || sqlDbType == SqlDbType.NVarChar) && iMaxLength > 0)
                iMaxLength = iMaxLength / 2;

            if (iMaxLength > 0 && sqlDbType != SqlDbType.VarChar && sqlDbType != SqlDbType.NVarChar &&
                sqlDbType != SqlDbType.Char && sqlDbType != SqlDbType.NChar &&
                sqlDbType != SqlDbType.Binary && sqlDbType != SqlDbType.VarBinary)
                iMaxLength = 0;

            return iMaxLength;
        }

        /// <summary>
        /// Gets a Sql type name from a DbType enum value
        /// </summary>
        public override string GetStringFromDbType(DbType dbType)
        {
            switch (dbType)
            {
                case DbType.AnsiString:
                case DbType.AnsiStringFixedLength:
                    return "varchar";
                case DbType.Binary:
                    return "varbinary";
                case DbType.Boolean:
                    return "bit";
                case DbType.Byte:
                    return "tinyint";
                case DbType.Currency:
                    return "money";
                case DbType.Date:
                    return "date";
                case DbType.DateTime:
                    return "datetime";
                case DbType.DateTime2:
                    return "datetime2";
                case DbType.DateTimeOffset:
                    return "datetimeoffset";
                case DbType.Decimal:
                case DbType.Double:
                case DbType.Single:
                    return "decimal";
                case DbType.VarNumeric:
                    return "numeric";
                case DbType.Guid:
                    return "uniqueidentifier";
                case DbType.Int16:
                    return "smallint";
                case DbType.Int32:
                case DbType.UInt16:
                    return "int";
                case DbType.Int64:
                case DbType.UInt32:
                case DbType.UInt64:
                    return "bigint";
                case DbType.SByte:
                    return "smallint";
                case DbType.String:
                case DbType.StringFixedLength:
                case DbType.Xml:
                    return "nvarchar";
                case DbType.Time:
                    return "time";
                case DbType.Object:
                    return "sql_variant";
            }
            throw new Exception($"this DbType {dbType.ToString()} is not supported");
        }

        /// <summary>
        /// Gets a Sql type name form a SqlDbType enum value
        /// </summary>
        public override string GetStringFromOwnerDbType(object ownerType)
        {
            SqlDbType sqlDbType = (SqlDbType)ownerType;

            switch (sqlDbType)
            {
                case SqlDbType.BigInt:
                    return "bigint";
                case SqlDbType.Binary:
                    return "binary";
                case SqlDbType.Bit:
                    return "bit";
                case SqlDbType.Char:
                    return "char";
                case SqlDbType.Date:
                    return "date";
                case SqlDbType.DateTime:
                    return "datetime";
                case SqlDbType.DateTime2:
                    return "datetime2";
                case SqlDbType.DateTimeOffset:
                    return "datetimeoffset";
                case SqlDbType.Decimal:
                    return "decimal";
                case SqlDbType.Float:
                    return "float";
                case SqlDbType.Int:
                    return "int";
                case SqlDbType.Money:
                    return "money";
                case SqlDbType.NChar:
                    return "nchar";
                case SqlDbType.NVarChar:
                    return "nvarchar";
                case SqlDbType.Real:
                    return "real";
                case SqlDbType.SmallDateTime:
                    return "smalldatetime";
                case SqlDbType.SmallInt:
                    return "smallint";
                case SqlDbType.SmallMoney:
                    return "smallmoney";
                case SqlDbType.Time:
                    return "time";
                case SqlDbType.Timestamp:
                    return "timestamp";
                case SqlDbType.TinyInt:
                    return "tinyint";
                case SqlDbType.UniqueIdentifier:
                    return "uniqueidentifier";
                case SqlDbType.VarBinary:
                    return "varbinary";
                case SqlDbType.VarChar:
                    return "varchar";
                case SqlDbType.Variant:
                    return "sql_variant";
                case SqlDbType.Xml:
                    return "xml";
            }
            throw new Exception($"this SqlDbType {ownerType.ToString()} is not supported");
        }


        public override string GetPrecisionStringFromDbType(DbType dbType, int maxLength, byte precision, byte scale)
        {
            switch (dbType)
            {
                case DbType.AnsiString:
                    if (maxLength > 0 && maxLength < 8000)
                        return $"({maxLength})";
                    else
                        return $"(MAX)";
                case DbType.String:
                    if (maxLength > 0 && maxLength < 4000)
                        return $"({maxLength})";
                    else
                        return $"(MAX)";
                case DbType.AnsiStringFixedLength:
                    if (maxLength > 0 && maxLength < 4000)
                        return $"({maxLength})";
                    else
                        return string.Empty;
                case DbType.StringFixedLength:
                case DbType.Binary:
                    if (maxLength > 0 && maxLength < 4000)
                        return $"({maxLength})";
                    else
                        return string.Empty;
                case DbType.Decimal:
                case DbType.Double:
                case DbType.Single:
                case DbType.VarNumeric:
                    var (p, s) = CoercePrecisionAndScale(precision, scale);

                    if (p > 0 && s <= 0)
                        return $"({ p})";
                    else if (p > 0 && s > 0)
                        return $"({ p}, {s})";
                    else
                        return string.Empty;
            }
            return string.Empty;

        }

        private static (byte p, byte s) CoercePrecisionAndScale(int precision, int scale)
        {
            byte p = Convert.ToByte(precision);
            byte s = Convert.ToByte(scale);
            if (p > PRECISION_MAX)
            {
                p = PRECISION_MAX;
                //s = SCALE_MAX;
            }

            if (s > SCALE_MAX)
            {
                s = SCALE_MAX;
            }
            // scale should always be lesser than precision
            if (s >= p)
            {
                s = (byte)(p - 1);
            }

            return (p, s);
        }

        /// <summary>
        /// return the precision | maxlength string used when generating scripts
        /// </summary>
        public override string GetPrecisionStringFromOwnerDbType(object ownerDbType, int maxLength, byte precision, byte scale)
        {
            SqlDbType sqlDbType = (SqlDbType)ownerDbType;
            switch (sqlDbType)
            {
                case SqlDbType.NVarChar:
                    if (maxLength > 0 && maxLength < 4000)
                        return $"({maxLength})";
                    else
                        return "(MAX)";
                case SqlDbType.VarBinary:
                case SqlDbType.VarChar:
                    if (maxLength > 0 && maxLength < 8000)
                        return $"({maxLength})";
                    else
                        return "(MAX)";
                case SqlDbType.NChar:
                    if (maxLength > 0 && maxLength < 4000)
                        return $"({maxLength})";
                    else
                        return string.Empty;
                case SqlDbType.Char:
                case SqlDbType.Binary:
                    if (maxLength > 0 && maxLength < 4000)
                        return $"({maxLength})";
                    else
                        return string.Empty;

                case SqlDbType.Decimal:
                    var (p, s) = CoercePrecisionAndScale(precision, scale);

                    if (p > 0 && s <= 0)
                        return $"({ p})";
                    else if (p > 0 && s > 0)
                        return $"({ p}, {s})";
                    else
                        return string.Empty;
                default:
                    return string.Empty;
            }
        }

        /// <summary>
        /// Gets the corresponding SqlDbType from a classic DbType
        /// </summary>
        public override object GetOwnerDbTypeFromDbType(DbType dbType)
        {
            // Fallback on DbType
            switch (dbType)
            {
                case DbType.AnsiString:
                case DbType.AnsiStringFixedLength:
                    return SqlDbType.VarChar;
                case DbType.Binary:
                    return SqlDbType.VarBinary;
                case DbType.Boolean:
                    return SqlDbType.Bit;
                case DbType.Byte:
                    return SqlDbType.TinyInt;
                case DbType.Currency:
                    return SqlDbType.Money;
                case DbType.Date:
                    return SqlDbType.Date;
                case DbType.DateTime:
                    return SqlDbType.DateTime;
                case DbType.DateTime2:
                    return SqlDbType.DateTime2;
                case DbType.DateTimeOffset:
                    return SqlDbType.DateTimeOffset;
                case DbType.Decimal:
                case DbType.Double:
                case DbType.Single:
                case DbType.VarNumeric:
                    return SqlDbType.Decimal;
                case DbType.Guid:
                    return SqlDbType.UniqueIdentifier;
                case DbType.Int16:
                    return SqlDbType.SmallInt;
                case DbType.Int32:
                case DbType.UInt16:
                    return SqlDbType.Int;
                case DbType.Int64:
                case DbType.UInt32:
                case DbType.UInt64:
                    return SqlDbType.BigInt;
                case DbType.SByte:
                    return SqlDbType.SmallInt;
                case DbType.String:
                case DbType.StringFixedLength:
                case DbType.Xml:
                    return SqlDbType.NVarChar;
                case DbType.Time:
                    return SqlDbType.Time;
                case DbType.Object:
                    return SqlDbType.Variant;
            }

            throw new Exception($"this type {dbType} is not supported");

        }

        /// <summary>
        /// Gets a managed type from a SqlDbType
        /// </summary>
        public override Type ValidateType(object ownerType)
        {
            SqlDbType sqlDbType = (SqlDbType)ownerType;

            switch (sqlDbType)
            {
                case SqlDbType.BigInt:
                    return Type.GetType("System.Int64");
                case SqlDbType.Binary:
                    return Type.GetType("System.Byte[]");
                case SqlDbType.Bit:
                    return Type.GetType("System.Boolean");
                case SqlDbType.Char:
                    return Type.GetType("System.String");
                case SqlDbType.Date:
                    return Type.GetType("System.DateTime");
                case SqlDbType.DateTime:
                    return Type.GetType("System.DateTime");
                case SqlDbType.DateTime2:
                    return Type.GetType("System.DateTime");
                case SqlDbType.DateTimeOffset:
                    return Type.GetType("System.DateTimeOffset");
                case SqlDbType.Decimal:
                    return Type.GetType("System.Decimal");
                case SqlDbType.Float:
                    return Type.GetType("System.Double");
                case SqlDbType.Int:
                    return Type.GetType("System.Int32");
                case SqlDbType.Money:
                    return Type.GetType("System.Decimal");
                case SqlDbType.NChar:
                    return Type.GetType("System.String");
                case SqlDbType.NVarChar:
                    return Type.GetType("System.String");
                case SqlDbType.Real:
                    return Type.GetType("System.Single");
                case SqlDbType.SmallDateTime:
                    return Type.GetType("System.DateTime");
                case SqlDbType.SmallInt:
                    return Type.GetType("System.Int16");
                case SqlDbType.SmallMoney:
                    return Type.GetType("System.Decimal");
                case SqlDbType.Structured:
                    return Type.GetType("System.Byte[]");
                case SqlDbType.Time:
                    return Type.GetType("System.TimeSpan");
                case SqlDbType.Timestamp:
                    return Type.GetType("System.Byte[]");
                case SqlDbType.TinyInt:
                    return Type.GetType("System.Byte");
                case SqlDbType.Udt:
                    return Type.GetType("System.Byte[]");
                case SqlDbType.UniqueIdentifier:
                    return Type.GetType("System.Guid");
                case SqlDbType.VarBinary:
                    return Type.GetType("System.Byte[]");
                case SqlDbType.VarChar:
                    return Type.GetType("System.String");
                case SqlDbType.Variant:
                    return Type.GetType("System.Object");
                case SqlDbType.Xml:
                    return Type.GetType("System.String");
            }
            throw new Exception($"this SqlDbType {ownerType.ToString()} is not supported");
        }

        public override bool SupportScale(string typeName)
        {
            switch (typeName.ToLowerInvariant())
            {
                case "decimal":
                case "real":
                case "float":
                case "numeric":
                case "money":
                case "smallmoney":
                    return true;
            }
            return false;
        }
        public override bool IsNumericType(string typeName)
        {
            switch (typeName.ToLowerInvariant())
            {
                case "bigint":
                case "decimal":
                case "float":
                case "int":
                case "numeric":
                case "real":
                case "smallint":
                case "tinyint":
                case "money":
                case "smallmoney":
                    return true;
            }
            return false;
        }

        public override bool IsTextType(string typeName)
        {
            switch (typeName.ToLowerInvariant())
            {
                case "char":
                case "nchar":
                case "nvarchar":
                case "varchar":
                case "xml":
                    return true;
            }
            return false;
        }

        public override bool IsValid(SyncColumn columnDefinition)
        {
            switch (columnDefinition.OriginalTypeName.ToLowerInvariant())
            {
                case "bigint":
                case "binary":
                case "bit":
                case "char":
                case "date":
                case "datetime":
                case "datetime2":
                case "datetimeoffset":
                case "decimal":
                case "float":
                case "int":
                case "money":
                case "nchar":
                case "numeric":
                case "nvarchar":
                case "real":
                case "smalldatetime":
                case "smallint":
                case "smallmoney":
                case "sql_variant":
                case "variant":
                case "time":
                case "timestamp":
                case "tinyint":
                case "uniqueidentifier":
                case "varbinary":
                case "varchar":
                case "xml":
                    return true;
            }
            return false;
        }

       

        public override bool ValidateIsReadonly(SyncColumn columnDefinition)
        {
            return columnDefinition.OriginalTypeName.ToLowerInvariant() == "timestamp" ||
                   columnDefinition.IsCompute;
        }

        public override byte ValidatePrecision(SyncColumn columnDefinition)
        {
            var (p, s) = CoercePrecisionAndScale(columnDefinition.Precision, columnDefinition.Scale);
            
            return p;
        }

        public override (byte precision, byte scale) GetPrecisionFromOwnerDbType(object ownerDbType, byte precision, byte scale)
        {
            SqlDbType sqlDbType = (SqlDbType)ownerDbType;
            switch (sqlDbType)
            {
                case SqlDbType.Decimal:
                    return CoercePrecisionAndScale(precision, scale);
            }
            return (0, 0);
        }

        public override (byte precision, byte scale) GetPrecisionFromDbType(DbType dbType, byte precision, byte scale)
        {
            switch (dbType)
            {
                case DbType.Decimal:
                case DbType.Double:
                case DbType.Single:
                case DbType.VarNumeric:
                    return CoercePrecisionAndScale(precision, scale);
            }
            return (0, 0);
        }

        public override int GetMaxLengthFromDbType(DbType dbType, int maxLength)
        {
            switch (dbType)
            {
                case DbType.AnsiString:
                case DbType.AnsiStringFixedLength:
                case DbType.Binary:
                case DbType.String:
                case DbType.StringFixedLength:
                    return maxLength;
            }
            return 0;
        }

        public override int GetMaxLengthFromOwnerDbType(object ownerDbType, int maxLength)
        {
            SqlDbType sqlDbType = (SqlDbType)ownerDbType;
            switch (sqlDbType)
            {
                case SqlDbType.Binary:
                case SqlDbType.Char:
                case SqlDbType.NChar:
                case SqlDbType.NVarChar:
                case SqlDbType.VarBinary:
                case SqlDbType.VarChar:
                    return maxLength;
            }
            return 0;
        }

        public override (byte precision, byte scale) ValidatePrecisionAndScale(SyncColumn columnDefinition)
        {
            return CoercePrecisionAndScale(columnDefinition.Precision, columnDefinition.Scale);
        }
    }

}

