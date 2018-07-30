using Dotmim.Sync.Data;
using Dotmim.Sync.Manager;
using System;
using System.Data;
using System.Data.OracleClient;

namespace Dotmim.Sync.Oracle.Manager
{
    public class OracleDbMetadata : DbMetadata
    {
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

        public override int GetMaxLengthFromOwnerDbType(object dbType, int maxLength)
        {
            OracleType sqlDbType = (OracleType)dbType;
            switch (sqlDbType)
            {
                case OracleType.Blob:
                case OracleType.Char:
                case OracleType.NChar:
                case OracleType.NVarChar:
                case OracleType.VarChar:
                    return maxLength;
            }
            return 0;
        }

        public override object GetOwnerDbTypeFromDbType(DbType dbType)
        {
            switch (dbType)
            {
                case DbType.AnsiString:
                case DbType.AnsiStringFixedLength:
                    return OracleType.VarChar;
                case DbType.Binary:
                    return OracleType.Blob;
                case DbType.Boolean:
                    return OracleType.Byte;
                case DbType.Byte:
                    return OracleType.Int16;
                case DbType.Date:
                    return OracleType.DateTime;
                case DbType.DateTime:
                    return OracleType.DateTime;
                case DbType.DateTime2:
                    return OracleType.DateTime;
                case DbType.Decimal:
                case DbType.Double:
                case DbType.Single:
                case DbType.VarNumeric:
                    return OracleType.Double;
                case DbType.Int16:
                    return OracleType.Int16;
                case DbType.Int32:
                case DbType.UInt16:
                    return OracleType.Int32;
                case DbType.Int64:
                case DbType.UInt32:
                case DbType.UInt64:
                    return OracleType.Number;
                case DbType.String:
                case DbType.StringFixedLength:
                case DbType.Xml:
                    return OracleType.NVarChar;
                case DbType.Time:
                    return OracleType.Timestamp;
            }

            throw new Exception($"this type {dbType} is not supported");
        }

        public override (byte precision, byte scale) GetPrecisionFromDbType(DbType dbType, byte precision, byte scale)
        {
            switch (dbType)
            {
                case DbType.Decimal:
                case DbType.Double:
                case DbType.Single:
                case DbType.VarNumeric:
                    return (precision, scale);
            }
            return (0, 0);
        }

        public override (byte precision, byte scale) GetPrecisionFromOwnerDbType(object dbType, byte precision, byte scale)
        {
            OracleType sqlDbType = (OracleType)dbType;
            switch (sqlDbType)
            {
                case OracleType.Double:
                    return (precision, scale);
            }
            return (0, 0);
        }

        public override string GetPrecisionStringFromDbType(DbType dbType, int maxLength, byte precision, byte scale)
        {
            switch (dbType)
            {
                case DbType.AnsiString:
                    if (maxLength > 0)
                        return $"({maxLength})";
                    else
                        return $"(MAX)";
                case DbType.String:
                    if (maxLength > 0)
                        return $"({Math.Min(maxLength, 4000)})";
                    else
                        return $"(MAX)";
                case DbType.AnsiStringFixedLength:
                    if (maxLength > 0)
                        return $"({Math.Min(maxLength, 4000)})";
                    else
                        return string.Empty;
                case DbType.StringFixedLength:
                case DbType.Binary:
                    if (maxLength > 0)
                        return $"({maxLength})";
                    else
                        return string.Empty;
                case DbType.Decimal:
                case DbType.Double:
                case DbType.Single:
                case DbType.VarNumeric:
                    if (precision > 0 && scale <= 0)
                        return $"({ precision})";
                    else if (precision > 0 && scale > 0)
                        return $"({ precision}, {scale})";
                    else
                        return string.Empty;
            }
            return string.Empty;
        }

        public override string GetPrecisionStringFromOwnerDbType(object dbType, int maxLength, byte precision, byte scale)
        {
            OracleType sqlDbType = (OracleType)dbType;
            switch (sqlDbType)
            {
                case OracleType.NVarChar:
                    if (maxLength > 0)
                        return $"({Math.Min(maxLength, 4000)})";
                    else
                        return "(200)";
                case OracleType.Blob:
                case OracleType.VarChar:
                    if (maxLength > 0)
                        return $"({maxLength})";
                    else
                        return "(MAX)";
                case OracleType.NChar:
                    if (maxLength > 0)
                        return $"({Math.Min(maxLength, 4000)})";
                    else
                        return string.Empty;
                case OracleType.Char:
                    if (maxLength > 0)
                        return $"({maxLength})";
                    else
                        return string.Empty;

                case OracleType.Number:
                    if (precision > 0 && scale <= 0)
                        return $"({ precision})";
                    else if (precision > 0 && scale > 0)
                        return $"({ precision}, {scale})";
                    else
                        return string.Empty;
                default:
                    return string.Empty;
            }
        }

        public override string GetStringFromDbType(DbType dbType)
        {
            switch (dbType)
            {
                case DbType.AnsiString:
                case DbType.AnsiStringFixedLength:
                    return "varchar";
                case DbType.Binary:
                    return "blob";
                case DbType.Boolean:
                    return "number";
                case DbType.Byte:
                    return "number";
                case DbType.Date:
                    return "date";
                case DbType.DateTime:
                    return "datetime";
                case DbType.Decimal:
                case DbType.Double:
                case DbType.Single:
                    return "number";
                case DbType.String:
                case DbType.StringFixedLength:
                case DbType.Xml:
                    return "nvarchar";
            }
            throw new Exception($"this DbType {dbType.ToString()} is not supported");
        }

        public override string GetStringFromOwnerDbType(object ownerType)
        {
            OracleType sqlDbType = (OracleType)ownerType;

            switch (sqlDbType)
            {
                case OracleType.Cursor:
                    return "sys_refcursor";
                case OracleType.Number:
                    return "number";
                case OracleType.Blob:
                    return "binary";
                case OracleType.Char:
                    return "char";
                case OracleType.DateTime:
                    return "datetime";
                case OracleType.Float:
                    return "float";
                case OracleType.NChar:
                    return "nchar";
                case OracleType.NVarChar:
                    return "nvarchar";
                case OracleType.Timestamp:
                    return "timestamp";
                case OracleType.VarChar:
                    return "varchar";
            }
            throw new Exception($"this SqlDbType {ownerType.ToString()} is not supported");
        }

        public override bool IsNumericType(string typeName)
        {
            switch (typeName.ToLowerInvariant())
            {
                case "number":
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
                case "nvarchar2":
                    return true;
            }
            return false;
        }

        public override bool IsValid(DmColumn columnDefinition)
        {
            switch (columnDefinition.OriginalTypeName.ToLowerInvariant())
            {
                case "number":
                case "date":
                case "datetime":
                case "char":
                case "datetime2":
                case "nchar":
                case "nvarchar":
                case "nvarchar2":
                case "varchar2":
                case "timestamp":
                case "blob":
                case "clob":
                case "varchar":
                    return true;
            }
            return false;
        }

        public override bool SupportScale(string typeName)
        {
            switch (typeName.ToLowerInvariant())
            {
                case "float":
                    return true;
            }
            return false;
        }

        public override DbType ValidateDbType(string typeName, bool isUnsigned, bool isUnicode)
        {
            switch (typeName.ToLowerInvariant())
            {
                case "nvarchar":
                    return DbType.String;
                case "nvarchar2":
                    return DbType.String;
                case "varchar2":
                    return DbType.String;
                case "char":
                    return DbType.AnsiStringFixedLength;
                case "date":
                    return DbType.Date;
                case "datetime":
                    return DbType.DateTime;
                case "datetime2":
                    return DbType.DateTime2;
                case "number":
                    return DbType.Int64;
                case "nchar":
                    return DbType.StringFixedLength;
                case "float":
                    return DbType.Double;
                case "timestamp":
                    return DbType.Int64;
                case "varchar":
                    return DbType.AnsiString;
            }
            throw new Exception($"this type name {typeName} is not supported");
        }

        public override bool ValidateIsReadonly(DmColumn columnDefinition) => false;

        public override int ValidateMaxLength(string typeName, bool isUnsigned, bool isUnicode, long maxLength)
        {
            OracleType sqlDbType = (OracleType)ValidateOwnerDbType(typeName, isUnsigned, isUnicode);

            Int32 iMaxLength = maxLength > 8000 ? 8000 : Convert.ToInt32(maxLength);

            // special length for nchar and nvarchar
            if ((sqlDbType == OracleType.NChar || sqlDbType == OracleType.NVarChar) && iMaxLength > 0)
                iMaxLength = iMaxLength / 2;

            if (iMaxLength > 0 && sqlDbType != OracleType.VarChar && sqlDbType != OracleType.NVarChar &&
                sqlDbType != OracleType.Char && sqlDbType != OracleType.NChar &&
                sqlDbType != OracleType.Blob && sqlDbType != OracleType.Clob)
                iMaxLength = 0;

            return iMaxLength;
        }

        public override object ValidateOwnerDbType(string typeName, bool isUnsigned, bool isUnicode)
        {
            switch (typeName.ToLowerInvariant())
            {
                case "number":
                    return OracleType.Number;
                case "bigint":
                    return OracleType.Number;
                case "binary":
                    return OracleType.Blob;
                case "bit":
                    return OracleType.Byte;
                case "char":
                    return OracleType.Char;
                case "date":
                    return OracleType.DateTime;
                case "datetime":
                    return OracleType.DateTime;
                case "datetime2":
                    return OracleType.DateTime;
                case "decimal":
                    return OracleType.Double;
                case "float":
                    return OracleType.Float;
                case "int":
                    return OracleType.Int32;
                case "nchar":
                    return OracleType.NChar;
                case "numeric":
                    return OracleType.Float;
                case "nvarchar":
                    return OracleType.NVarChar;
                case "nvarchar2":
                    return OracleType.NVarChar;
                case "varchar2":
                    return OracleType.VarChar;
                case "time":
                    return OracleType.Timestamp;
                case "timestamp":
                    return OracleType.Timestamp;
                case "varchar":
                    return OracleType.VarChar;
            }
            throw new Exception($"this type name {typeName} is not supported");
        }

        public override byte ValidatePrecision(DmColumn columnDefinition)
        {
            return columnDefinition.Precision;
        }

        public override (byte precision, byte scale) ValidatePrecisionAndScale(DmColumn columnDefinition)
        {
            return (columnDefinition.Precision, columnDefinition.Scale);
        }

        public override Type ValidateType(object ownerType)
        {
            OracleType sqlDbType = (OracleType)ownerType;

            switch (sqlDbType)
            {
                case OracleType.Number:
                    return Type.GetType("System.Int32");
                case OracleType.Int32:
                    return Type.GetType("System.Int32");
                case OracleType.Blob:
                    return Type.GetType("System.Byte[]");
                case OracleType.Clob:
                    return Type.GetType("System.String");
                case OracleType.Byte:
                    return Type.GetType("System.Boolean");
                case OracleType.Char:
                    return Type.GetType("System.String");
                case OracleType.DateTime:
                    return Type.GetType("System.DateTime");
                case OracleType.Float:
                    return Type.GetType("System.Float");
                case OracleType.Double:
                    return Type.GetType("System.Double");
                case OracleType.Int16:
                    return Type.GetType("System.Int16");
                case OracleType.NChar:
                    return Type.GetType("System.String");
                case OracleType.NVarChar:
                    return Type.GetType("System.String");
                case OracleType.VarChar:
                    return Type.GetType("System.String");
            }

            throw new Exception($"this SqlDbType {ownerType.ToString()} is not supported");
        }

        public OracleType ValidateOracleType(Type ownerType)
        {
            switch (ownerType.ToString())
            {
                case "System.Int16":
                case "System.Int32":
                case "System.Int64":
                    return OracleType.Number;
                case "System.Byte[]":
                    return OracleType.Blob;
                case "System.String":
                case "System.Guid":
                    return OracleType.NVarChar;
                case "System.Boolean":
                    return OracleType.Byte;
                case "System.DateTime":
                    return OracleType.DateTime;
                case "System.Float":
                    return OracleType.Float;
                case "System.Double":
                    return OracleType.Double;
            }

            throw new Exception($"this SqlDbType {ownerType.ToString()} is not supported");
        }
    }
}
