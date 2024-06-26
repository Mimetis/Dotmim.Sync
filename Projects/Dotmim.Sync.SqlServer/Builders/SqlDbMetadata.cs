﻿using Dotmim.Sync.Manager;
using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Linq;
using System.Data.Common;


namespace Dotmim.Sync.SqlServer.Manager
{
    public class SqlDbMetadata : DbMetadata
    {
        // Even if precision max can be 38 on SQL Server, prefer go for 28, to not having a truncation
        // public const Byte PRECISION_MAX = 28;
        // 2021/02/16 : Trying to resverse back to 38
        public const byte PRECISION_MAX = 38;
        public const byte PRECISION_DEFAULT = 22;
        public const byte SCALE_DEFAULT = 8;
        public const byte SCALE_MAX = 18;

        public SqlDbMetadata() { }

        /// <summary>
        /// Gets the DbType issue from the database
        /// </summary>
        public override DbType GetDbType(SyncColumn column) => column.OriginalTypeName.ToLowerInvariant() switch
        {
            "bigint" => DbType.Int64,
            "binary" => DbType.Binary,
            "bit" => DbType.Boolean,
            "char" => DbType.AnsiStringFixedLength,
            "date" => DbType.Date,
            "datetime" => DbType.DateTime,
            "datetime2" => DbType.DateTime2,
            "datetimeoffset" => DbType.DateTimeOffset,
            "decimal" => DbType.Decimal,
            "float" => DbType.Double,
            "int" => DbType.Int32,
            "money" => DbType.Currency,
            "smallmoney" => DbType.Currency,
            "nchar" => DbType.StringFixedLength,
            "numeric" => DbType.VarNumeric,
            "nvarchar" => DbType.String,
            "real" => DbType.Decimal,
            "smalldatetime" => DbType.DateTime,
            "smallint" => DbType.Int16,
            "sql_variant" => DbType.Object,
            "variant" => DbType.Object,
            "time" => DbType.Time,
            "timestamp" => DbType.Int64,
            "tinyint" => DbType.Int16,
            "uniqueidentifier" => DbType.Guid,
            "varbinary" => DbType.Binary,
            "varchar" => DbType.AnsiString,
            "xml" => DbType.String,
            _ => throw new Exception($"this type {column.OriginalTypeName} for column {column.ColumnName} is not supported")
        };

        /// <summary>
        /// Gets the SqlDbType issued from the database
        /// </summary>
        public override object GetOwnerDbType(SyncColumn column) => column.OriginalTypeName.ToLowerInvariant() switch
        {
            "bigint" => SqlDbType.BigInt,
            "binary" => SqlDbType.Binary,
            "bit" => SqlDbType.Bit,
            "char" => SqlDbType.Char,
            "date" => SqlDbType.Date,
            "datetime" => SqlDbType.DateTime,
            "datetime2" => SqlDbType.DateTime2,
            "datetimeoffset" => SqlDbType.DateTimeOffset,
            "decimal" => SqlDbType.Decimal,
            "float" => SqlDbType.Float,
            "int" => SqlDbType.Int,
            "money" => SqlDbType.Money,
            "smallmoney" => SqlDbType.SmallMoney,
            "nchar" => SqlDbType.NChar,
            "numeric" => SqlDbType.Decimal,
            "nvarchar" => SqlDbType.NVarChar,
            "real" => SqlDbType.Real,
            "smalldatetime" => SqlDbType.SmallDateTime,
            "smallint" => SqlDbType.SmallInt,
            "sql_variant" => SqlDbType.Variant,
            "variant" => SqlDbType.Variant,
            "time" => SqlDbType.Time,
            "timestamp" => SqlDbType.Timestamp,
            "tinyint" => SqlDbType.TinyInt,
            "uniqueidentifier" => SqlDbType.UniqueIdentifier,
            "varbinary" => SqlDbType.VarBinary,
            "varchar" => SqlDbType.VarChar,
            "xml" => SqlDbType.Xml,
            _ => throw new Exception($"Type '{column.OriginalTypeName.ToLowerInvariant()}' (column {column.ColumnName}) is not supported"),
        };

        /// <summary>
        /// Gets the SqlDbType issued from the database
        /// </summary>
        public SqlDbType GetSqlDbType(SyncColumn column) => (SqlDbType)this.GetOwnerDbType(column);

        /// <summary>
        /// Gets the SqlDbType issued from the downgraded DbType
        /// </summary>
        public SqlDbType GetOwnerDbTypeFromDbType(SyncColumn column) => column.GetDbType() switch
        {
            DbType.AnsiString => SqlDbType.VarChar,
            DbType.AnsiStringFixedLength => SqlDbType.VarChar,
            DbType.Binary => SqlDbType.VarBinary,
            DbType.Boolean => SqlDbType.Bit,
            DbType.Byte => SqlDbType.TinyInt,
            DbType.Currency => SqlDbType.Money,
            DbType.Date => SqlDbType.Date,
            DbType.DateTime => SqlDbType.DateTime,
            DbType.DateTime2 => SqlDbType.DateTime2,
            DbType.DateTimeOffset => SqlDbType.DateTimeOffset,
            DbType.Decimal => SqlDbType.Decimal,
            DbType.Double => SqlDbType.Decimal,
            DbType.Guid => SqlDbType.UniqueIdentifier,
            DbType.Int16 => SqlDbType.SmallInt,
            DbType.Int32 => SqlDbType.Int,
            DbType.Int64 => SqlDbType.BigInt,
            DbType.Object => SqlDbType.Variant,
            DbType.SByte => SqlDbType.SmallInt,
            DbType.Single => SqlDbType.Decimal,
            DbType.String => SqlDbType.NVarChar,
            DbType.StringFixedLength => SqlDbType.NVarChar,
            DbType.Time => SqlDbType.Time,
            DbType.UInt16 => SqlDbType.Int,
            DbType.UInt32 => SqlDbType.BigInt,
            DbType.UInt64 => SqlDbType.BigInt,
            DbType.VarNumeric => SqlDbType.Decimal,
            DbType.Xml => SqlDbType.NVarChar,
            _ => throw new Exception($"this db type {column.GetDbType()} for column {column.ColumnName} is not supported")
        };

        /// <summary>
        /// Gets a managed type from a SqlDbType
        /// </summary>
        public override Type GetType(SyncColumn column) => GetSqlDbType(column) switch
        {
            SqlDbType.BigInt => Type.GetType("System.Int64"),
            SqlDbType.Binary => Type.GetType("System.Byte[]"),
            SqlDbType.Bit => Type.GetType("System.Boolean"),
            SqlDbType.Char => Type.GetType("System.String"),
            SqlDbType.Date => Type.GetType("System.DateTime"),
            SqlDbType.DateTime => Type.GetType("System.DateTime"),
            SqlDbType.DateTime2 => Type.GetType("System.DateTime"),
            SqlDbType.DateTimeOffset => Type.GetType("System.DateTimeOffset"),
            SqlDbType.Decimal => Type.GetType("System.Decimal"),
            SqlDbType.Float => Type.GetType("System.Double"),
            SqlDbType.Int => Type.GetType("System.Int32"),
            SqlDbType.Money => Type.GetType("System.Decimal"),
            SqlDbType.NChar => Type.GetType("System.String"),
            SqlDbType.NVarChar => Type.GetType("System.String"),
            SqlDbType.Real => Type.GetType("System.Single"),
            SqlDbType.SmallDateTime => Type.GetType("System.DateTime"),
            SqlDbType.SmallInt => Type.GetType("System.Int16"),
            SqlDbType.SmallMoney => Type.GetType("System.Decimal"),
            SqlDbType.Structured => Type.GetType("System.Byte[]"),
            SqlDbType.Time => Type.GetType("System.TimeSpan"),
            SqlDbType.Timestamp => Type.GetType("System.Byte[]"),
            SqlDbType.TinyInt => Type.GetType("System.Byte"),
            SqlDbType.Udt => Type.GetType("System.Byte[]"),
            SqlDbType.UniqueIdentifier => Type.GetType("System.Guid"),
            SqlDbType.VarBinary => Type.GetType("System.Byte[]"),
            SqlDbType.VarChar => Type.GetType("System.String"),
            SqlDbType.Variant => Type.GetType("System.Object"),
            SqlDbType.Xml => Type.GetType("System.String"),
            _ => throw new Exception($"In Column {column.ColumnName}, the type {GetSqlDbType(column)} is not supported"),
        };

        /// <summary>
        /// Gets the max length autorized
        /// </summary>
        public override int GetMaxLength(SyncColumn column)
        {
            var sqlDbType = GetSqlDbType(column);

            var iMaxLength = column.MaxLength > 8000 ? 8000 : Convert.ToInt32(column.MaxLength);

            // special length for nchar and nvarchar
            if ((sqlDbType == SqlDbType.NChar || sqlDbType == SqlDbType.NVarChar) && iMaxLength > 0)
                iMaxLength /= 2;

            if (iMaxLength > 0 && sqlDbType != SqlDbType.VarChar && sqlDbType != SqlDbType.NVarChar &&
                sqlDbType != SqlDbType.Char && sqlDbType != SqlDbType.NChar &&
                sqlDbType != SqlDbType.Binary && sqlDbType != SqlDbType.VarBinary)
                iMaxLength = 0;

            return iMaxLength;
        }

        public override (byte precision, byte scale) GetPrecisionAndScale(SyncColumn columnDefinition)
        {
            if ((columnDefinition.DbType == (int)DbType.Single || columnDefinition.DbType == (int)DbType.Decimal || columnDefinition.DbType == (int)DbType.VarNumeric) && columnDefinition.Precision == 0 && columnDefinition.Scale == 0)
                return (PRECISION_DEFAULT, SCALE_DEFAULT);

            return CoercePrecisionAndScale(columnDefinition.Precision, columnDefinition.Scale);

        }

        public override byte GetPrecision(SyncColumn columnDefinition)
        {
            var (p, _) = CoercePrecisionAndScale(columnDefinition.Precision, columnDefinition.Scale);

            return p;
        }

        public override bool IsSupportingScale(SyncColumn column) => column.OriginalTypeName.ToLowerInvariant() switch
        {
            "decimal" or "real" or "float" or "numeric" or "money" or "smallmoney" => true,
            _ => false,
        };
        public override bool IsNumericType(SyncColumn column) => column.OriginalTypeName.ToLowerInvariant() switch
        {
            "bigint" or "decimal" or "float" or "int" or "numeric" or "real" or "smallint" or "tinyint" or "money" or "smallmoney" => true,
            _ => false,
        };
        public override bool IsValid(SyncColumn columnDefinition) => columnDefinition.OriginalTypeName.ToLowerInvariant() switch
        {
            "bigint" or "binary" or "bit" or "char" or "date" or "datetime" or "datetime2" or "datetimeoffset" or "decimal"
            or "float" or "int" or "money" or "nchar" or "numeric" or "nvarchar" or "real" or "smalldatetime" or "smallint" or "smallmoney"
            or "sql_variant" or "variant" or "time" or "timestamp" or "tinyint" or "uniqueidentifier" or "varbinary" or "varchar" or "xml" => true,
            _ => false,
        };
        public override bool IsReadonly(SyncColumn columnDefinition)
            => string.Equals(columnDefinition.OriginalTypeName, "timestamp", SyncGlobalization.DataSourceStringComparison) || columnDefinition.IsCompute;

        //------------------------------------------------------------------------

        /// <summary>
        /// Gets a compatible column definition, like nvarchar(50), int, decimal(8,2)
        /// </summary>
        public string GetCompatibleColumnTypeDeclarationString(SyncColumn column, string fromProviderType)
        {
            string argument = string.Empty;

            // We get the sql db type from the original provider otherwise fallback on sql db type extract from simple db type
            // TODO : Find something better than string comparison for change tracking provider
            var sqlDbType = fromProviderType == SqlSyncProvider.ProviderType ||
                    fromProviderType == "SqlSyncChangeTrackingProvider, Dotmim.Sync.SqlServer.SqlSyncChangeTrackingProvider" ?
                this.GetSqlDbType(column) : this.GetOwnerDbTypeFromDbType(column);

            switch (sqlDbType)
            {
                case SqlDbType.NVarChar:
                    if (column.MaxLength > 0 && column.MaxLength <= 4000)
                        argument = $"({column.MaxLength})";
                    else
                        argument = "(MAX)";
                    break;
                case SqlDbType.VarBinary:
                case SqlDbType.VarChar:
                    if (column.MaxLength > 0 && column.MaxLength <= 8000)
                        argument = $"({column.MaxLength})";
                    else
                        argument = "(MAX)";
                    break;
                case SqlDbType.NChar:
                    argument = $"({Math.Min(4000, column.MaxLength)})";
                    break;
                case SqlDbType.Char:
                case SqlDbType.Binary:
                    argument = $"({Math.Min(8000, column.MaxLength)})";
                    break;
                case SqlDbType.Decimal:
                    var (p, s) = this.GetPrecisionAndScale(column);

                    if (p > 0 && s <= 0)
                        argument = $"({p})";
                    else if (p > 0 && s > 0)
                        argument = $"({p}, {s})";
                    break;
                default:
                    argument = string.Empty;
                    break;
            }
            // TODO : Find something better than string comparison for change tracking provider
            var isSameProvider = fromProviderType == SqlSyncProvider.ProviderType ||
                fromProviderType == "SqlSyncChangeTrackingProvider, Dotmim.Sync.SqlServer.SqlSyncChangeTrackingProvider";

            string typeName = isSameProvider ? column.OriginalTypeName.ToLowerInvariant() : sqlDbType.ToString().ToLowerInvariant();
            typeName = typeName == "variant" ? "sql_variant" : typeName;

            return string.IsNullOrEmpty(argument) ? typeName : $"{typeName} {argument}";

        }

        /// <summary>
        /// Gets a compatible precision and scale
        /// </summary>
        public (byte precision, byte scale) GetCompatibleColumnPrecisionAndScale(SyncColumn column, string fromProviderType)
        {
            // We get the sql db type from the original provider otherwise fallback on sql db type extract from simple db type

            // TODO : Find something better than string comparison for change tracking provider
            var isSameProvider = fromProviderType == SqlSyncProvider.ProviderType ||
                fromProviderType == "SqlSyncChangeTrackingProvider, Dotmim.Sync.SqlServer.SqlSyncChangeTrackingProvider";

            var sqlDbType = isSameProvider ?
                this.GetSqlDbType(column) : this.GetOwnerDbTypeFromDbType(column);

            return sqlDbType switch
            {
                SqlDbType.Decimal => CoercePrecisionAndScale(column.Precision, column.Scale),
                _ => (0, 0),
            };
        }

        public int GetCompatibleMaxLength(SyncColumn column, string fromProviderType)
        {
            // We get the sql db type from the original provider otherwise fallback on sql db type extract from simple db type
            // TODO : Find something better than string comparison for change tracking provider
            var isSameProvider = fromProviderType == SqlSyncProvider.ProviderType ||
                fromProviderType == "SqlSyncChangeTrackingProvider, Dotmim.Sync.SqlServer.SqlSyncChangeTrackingProvider";


            var sqlDbType = fromProviderType == SqlSyncProvider.ProviderType ?
                this.GetSqlDbType(column) : this.GetOwnerDbTypeFromDbType(column);

            return sqlDbType switch
            {
                SqlDbType.Binary or SqlDbType.Char or SqlDbType.NChar or SqlDbType.NVarChar or SqlDbType.VarBinary or SqlDbType.VarChar => column.MaxLength,
                _ => 0,
            };
        }

        /// <summary>
        /// Check precision and scale
        /// </summary>
        public static (byte p, byte s) CoercePrecisionAndScale(int precision, int scale)
        {
            byte p = Convert.ToByte(precision);
            byte s = Convert.ToByte(scale);
            if (p > PRECISION_MAX)
                p = PRECISION_MAX;

            if (s > SCALE_MAX)
                s = SCALE_MAX;

            // scale should always be lesser than precision
            if (s >= p && p > 1)
                s = (byte)(p - 1);

            return (p, s);
        }

    }

}

