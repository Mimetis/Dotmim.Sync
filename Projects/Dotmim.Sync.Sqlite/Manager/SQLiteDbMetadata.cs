using Dotmim.Sync.Manager;
using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using Dotmim.Sync.Data;
using Microsoft.Data.Sqlite;

namespace Dotmim.Sync.Sqlite
{
    public class SqliteDbMetadata : DbMetadata
    {
        public override int GetMaxLengthFromDbType(DbType dbType, int maxLength)
        {
            return 0;
        }

        public override int GetMaxLengthFromOwnerDbType(object dbType, int maxLength)
        {
            return GetMaxLengthFromDbType((DbType)dbType, maxLength);
        }

        public override object GetOwnerDbTypeFromDbType(DbType dbType)
        {
            return dbType;
        }

        public override (byte precision, byte scale) GetPrecisionFromDbType(DbType dbType, byte precision, byte scale)
        {
            return (0, 0);
        }

        public override (byte precision, byte scale) GetPrecisionFromOwnerDbType(object dbType, byte precision, byte scale)
        {
            return GetPrecisionFromDbType((DbType)dbType, precision, scale);
        }

        public override string GetPrecisionStringFromDbType(DbType dbType, int maxLength, byte precision, byte scale)
        {
            return string.Empty;
        }

        public override string GetPrecisionStringFromOwnerDbType(object dbType, int maxLength, byte precision, byte scale)
        {
            return GetPrecisionStringFromDbType((DbType)dbType, maxLength, precision, scale);
        }

        public override string GetStringFromDbType(DbType dbType)
        {
            switch (dbType)
            {
                case DbType.AnsiString:
                case DbType.AnsiStringFixedLength:
                case DbType.String:
                case DbType.StringFixedLength:
                case DbType.Xml:
                case DbType.Time:
                case DbType.DateTimeOffset:
                    return "text";
                case DbType.Guid:
                case DbType.Binary:
                case DbType.Object:
                    return "blob";
                case DbType.Boolean:
                case DbType.Byte:
                case DbType.Int16:
                case DbType.Int32:
                case DbType.UInt16:
                case DbType.Int64:
                case DbType.UInt32:
                case DbType.UInt64:
                case DbType.SByte:
                    return "integer";
                case DbType.Date:
                case DbType.DateTime:
                case DbType.DateTime2:
                    return "datetime";
                case DbType.Decimal:
                case DbType.Double:
                case DbType.Single:
                case DbType.Currency:
                case DbType.VarNumeric:
                    return "numeric";
            }
            throw new Exception($"this DbType {dbType.ToString()} is not supported");
        }

        public override string GetStringFromOwnerDbType(object ownerType)
        {
            return GetStringFromDbType((DbType)ownerType);
        }

        public override bool IsNumericType(string typeName)
        {
            typeName = typeName.ToLowerInvariant();
            
            if (typeName.Contains("("))
                typeName = typeName.Substring(0, typeName.IndexOf("("));

            return typeName == "numeric" || typeName == "decimal";
        }

        public override bool IsTextType(string typeName)
        {
            return typeName.ToLowerInvariant() == "text";
        }

        public bool IsTextType(DbType dbType)
        {
            switch (dbType)
            {
                case DbType.AnsiString:
                case DbType.AnsiStringFixedLength:
                case DbType.String:
                case DbType.StringFixedLength:
                case DbType.Xml:
                    return true;
            }
            return false;
        }

        public override bool IsValid(SyncColumn columnDefinition)
        {
            var typeName = columnDefinition.OriginalTypeName.ToLowerInvariant();

            if (typeName.Contains("("))
                typeName = typeName.Substring(0, typeName.IndexOf("("));

            switch (typeName)
            {
                case "integer":
                case "decimal":
                case "bit":
                case "bigint":
                case "numeric":
                case "blob":
                case "image":
                case "datetime":
                case "text":
                    return true;
            }
            return false;
        }

        public override bool SupportScale(string typeName)
        {
            return typeName.ToLowerInvariant() == "numeric" || typeName.ToLowerInvariant() == "decimal";
        }

        public override DbType ValidateDbType(string typeName, bool isUnsigned, bool isUnicode, long maxLength)
        {
            if (typeName.Contains("("))
                typeName = typeName.Substring(0, typeName.IndexOf("("));

            switch (typeName.ToLowerInvariant())
            {
                case "bit":
                    return DbType.Boolean;
                case "integer":
                case "bigint":
                    return DbType.Int64;
                case "numeric":
                    return DbType.Double;
                case "decimal":
                    return DbType.Decimal;
                case "blob":
                case "image":
                    return DbType.Binary;
                case "datetime":
                    return DbType.DateTime;
                case "text":
                    return DbType.String;

            }
            throw new Exception($"this type name {typeName} is not supported");
        }

        public override bool ValidateIsReadonly(SyncColumn columnDefinition)
        {
            return false;
        }

        public override int ValidateMaxLength(string typeName, bool isUnsigned, bool isUnicode, long maxLength)
        {
            Int32 iMaxLength = maxLength > Int32.MaxValue ? Int32.MaxValue : Convert.ToInt32(maxLength);
            return iMaxLength;
        }

        public override object ValidateOwnerDbType(string typeName, bool isUnsigned, bool isUnicode, long maxLength)
        {
            if (typeName.Contains("("))
                typeName = typeName.Substring(0, typeName.IndexOf("("));

            switch (typeName.ToLowerInvariant())
            {
                case "bit":
                case "integer":
                case "bigint":
                    return SqliteType.Integer;
                case "numeric":
                case "decimal":
                    return SqliteType.Real;
                case "blob":
                case "image":
                    return SqliteType.Blob;
                case "datetime":
                case "text":
                    return SqliteType.Text;

            }
            throw new Exception($"this type name {typeName} is not supported");
        }

        public override byte ValidatePrecision(SyncColumn columnDefinition)
        {
            return columnDefinition.Precision;
        }

        public override (byte precision, byte scale) ValidatePrecisionAndScale(SyncColumn columnDefinition)
        {
            return (columnDefinition.Precision, columnDefinition.Scale);
        }

        public override Type ValidateType(object ownerType)
        {
            DbType dbType = (DbType)ownerType;

            switch (dbType)
            {
                case DbType.AnsiString:
                case DbType.AnsiStringFixedLength:
                case DbType.String:
                case DbType.StringFixedLength:
                case DbType.Xml:
                    return typeof(String);
                case DbType.Guid:
                    return typeof(Guid);
                case DbType.Binary:
                    return typeof(Byte[]);
                case DbType.Object:
                    return typeof(Object);
                case DbType.Boolean:
                    return typeof(bool);
                case DbType.Byte:
                    return typeof(byte);
                case DbType.Int16:
                    return typeof(Int16);
                case DbType.Int32:
                    return typeof(Int32);
                case DbType.UInt16:
                    return typeof(UInt16);
                case DbType.Int64:
                    return typeof(Int64);
                case DbType.UInt32:
                    return typeof(UInt32);
                case DbType.UInt64:
                    return typeof(UInt64);
                case DbType.SByte:
                    return typeof(SByte);
                case DbType.Time:
                  //  return typeof(TimeSpan); -- Doesn't work, when trying to insert in sqlite. not IConvertible mechanism to blob
                    return typeof(String);
                case DbType.Date:
                case DbType.DateTime:
                case DbType.DateTime2:
                case DbType.DateTimeOffset:
                    return typeof(DateTime);
                case DbType.Decimal:
                    return typeof(Decimal);
                case DbType.Double:
                    return typeof(Double);
                case DbType.Single:
                    return typeof(Single);
                case DbType.Currency:
                case DbType.VarNumeric:
                    return typeof(Double);
            }
            throw new Exception($"this DbType {ownerType.ToString()} is not supported");
        }
    }
}
