﻿using Dotmim.Sync.Manager;
using System;
using System.Data;
#if NET6_0 || NET8_0
using MySqlConnector;
#elif NETSTANDARD
using MySql.Data.MySqlClient;
#endif

#if MARIADB
namespace Dotmim.Sync.MariaDB.Builders
#elif MYSQL
namespace Dotmim.Sync.MySql.Builders
#endif
{

    /// <summary>
    /// Represents a MySql Metadata checker.
    /// </summary>
    public class MySqlDbMetadata : DbMetadata
    {
        internal const byte PRECISIONMAX = 65;
        internal const byte PRECISIONDEFAULT = 22;
        internal const byte SCALEDEFAULT = 8;
        internal const byte SCALEMAX = 30;

        /// <summary>
        /// Check precision and scale.
        /// </summary>
        public static (byte Precision, byte Scale) CoercePrecisionAndScale(int precision, int scale)
        {
            byte p = Convert.ToByte(precision);
            byte s = Convert.ToByte(scale);
            if (p > PRECISIONMAX)
                p = PRECISIONMAX;

            if (s > SCALEMAX)
                s = SCALEMAX;

            // scale should always be lesser than precision
            if (s >= p && p > 1)
                s = (byte)(p - 1);

            return (p, s);
        }

        /// <inheritdoc/>
        public override DbType GetDbType(SyncColumn columnDefinition)
        {
            DbType stringDbType = columnDefinition.IsUnicode ? DbType.String : DbType.AnsiString;

            return columnDefinition.OriginalTypeName.ToLowerInvariant() switch
            {
                "int" or "integer" or "mediumint" => columnDefinition.IsUnsigned ? DbType.UInt32 : DbType.Int32,
                "int16" => DbType.Int16,
                "int24" or "int32" => DbType.Int32,
                "int64" => DbType.Int64,
                "uint16" => DbType.UInt16,
                "uint24" or "uint32" => DbType.UInt32,
                "uint64" => DbType.UInt64,
                "bit" => DbType.Boolean,
                "numeric" => DbType.VarNumeric,
                "decimal" or "dec" or "fixed" or "real" or "double" or "float" => DbType.Decimal,
                "tinyint" => DbType.Byte,
                "bigint" => columnDefinition.IsUnsigned ? DbType.UInt64 : DbType.Int64,
                "serial" => DbType.UInt64,
                "smallint" => columnDefinition.IsUnsigned ? DbType.UInt16 : DbType.Int16,
                "mediumtext" or "longtext" or "json" or "tinytext" => columnDefinition.IsUnicode ? DbType.String : DbType.AnsiString,
                "date" => DbType.Date,
                "datetime" or "newdate" => DbType.DateTime,
                "blob" or "longblob" or "tinyblob" or "mediumblob" or "binary" or "varbinary" => DbType.Binary,
                "year" => DbType.Int32,
                "time" => DbType.Time,
                "timestamp" => DbType.DateTime,
                "text" or "set" or "enum" or "nchar" or "nvarchar" or "varchar" or "json" => stringDbType,
                "char" => columnDefinition.MaxLength == 36 ? DbType.Guid : stringDbType,

                _ => throw new Exception($"this db type {columnDefinition.GetDbType()} for column {columnDefinition.ColumnName} is not supported"),
            };
        }

        /// <inheritdoc/>
        public override object GetOwnerDbType(SyncColumn columnDefinition) => columnDefinition.OriginalTypeName.ToLowerInvariant() switch
        {
            "char" => columnDefinition.MaxLength == 36 ? MySqlDbType.Guid : MySqlDbType.String,
            "guid" => MySqlDbType.Guid,
            "json" => MySqlDbType.JSON,
            "string" => MySqlDbType.String,
            "varchar" => MySqlDbType.VarChar,
            "date" => MySqlDbType.Date,
            "datetime" => MySqlDbType.DateTime,
            "numeric" or "decimal" or "dec" or "fixed" => MySqlDbType.Decimal,
            "year" => MySqlDbType.Year,
            "time" => MySqlDbType.Time,
            "timestamp" => MySqlDbType.Timestamp,
            "set" => MySqlDbType.Set,
            "enum" => MySqlDbType.Enum,
            "bit" => MySqlDbType.Bit,
            "byte" => MySqlDbType.Byte,
            "ubyte" => MySqlDbType.UByte,
            "tinyint" => MySqlDbType.Byte,
            "bool" or "boolean" => MySqlDbType.Byte,
            "smallint" => columnDefinition.IsUnsigned ? MySqlDbType.UInt16 : MySqlDbType.Int16,
            "mediumint" => columnDefinition.IsUnsigned ? MySqlDbType.UInt24 : MySqlDbType.Int24,
            "int" or "integer" => columnDefinition.IsUnsigned ? MySqlDbType.UInt32 : MySqlDbType.Int32,
            "serial" => MySqlDbType.UInt64,
            "bigint" => columnDefinition.IsUnsigned ? MySqlDbType.UInt64 : MySqlDbType.Int64,
            "unit16" => MySqlDbType.UInt16,
            "uint24" => MySqlDbType.UInt24,
            "uint32" => MySqlDbType.UInt32,
            "uint64" => MySqlDbType.UInt64,
            "int16" => MySqlDbType.Int16,
            "int24" => MySqlDbType.Int24,
            "int32" => MySqlDbType.Int32,
            "int64" => MySqlDbType.Int64,
            "float" => MySqlDbType.Float,
            "double" => MySqlDbType.Double,
            "real" => MySqlDbType.Float,
            "text" => MySqlDbType.Text,
            "blob" => MySqlDbType.Blob,
            "longblob" => MySqlDbType.LongBlob,
            "longtext" => MySqlDbType.LongText,
            "mediumblob" => MySqlDbType.MediumBlob,
            "mediumtext" => MySqlDbType.MediumText,
            "tinyblob" => MySqlDbType.TinyBlob,
            "tinytext" => MySqlDbType.TinyText,
            "binary" => MySqlDbType.Binary,
            "varbinary" => MySqlDbType.VarBinary,
            _ => throw new Exception($"Type '{columnDefinition.OriginalTypeName.ToLowerInvariant()}' (column {columnDefinition.ColumnName}) is not supported"),
        };

        /// <summary>
        /// Get a MySql DbType.
        /// </summary>
        public MySqlDbType GetMySqlDbType(SyncColumn column) => (MySqlDbType)this.GetOwnerDbType(column);

        /// <summary>
        /// Get a MySql DbTYpe.
        /// </summary>
        public MySqlDbType GetOwnerDbTypeFromDbType(SyncColumn columnDefinition) => columnDefinition.GetDbType() switch
        {
            DbType.AnsiString or DbType.Xml or DbType.String => MySqlDbType.LongText,
            DbType.StringFixedLength or DbType.AnsiStringFixedLength => MySqlDbType.VarChar,
            DbType.Binary => columnDefinition.MaxLength <= 0 || columnDefinition.MaxLength > 8000 ? MySqlDbType.LongBlob : MySqlDbType.VarBinary,
            DbType.Boolean => MySqlDbType.Bit,
            DbType.Byte => MySqlDbType.Byte,
            DbType.Currency => MySqlDbType.Decimal,
            DbType.Date => MySqlDbType.Date,
            DbType.DateTime or DbType.DateTime2 or DbType.DateTimeOffset => MySqlDbType.DateTime,
            DbType.Decimal => MySqlDbType.Decimal,
            DbType.Double => MySqlDbType.Double,
            DbType.Guid => MySqlDbType.Guid,
            DbType.Int16 => MySqlDbType.Int16,
            DbType.Int32 => MySqlDbType.Int32,
            DbType.Int64 => MySqlDbType.Int64,
            DbType.Object => MySqlDbType.LongBlob,
            DbType.SByte => MySqlDbType.Byte,
            DbType.Single => MySqlDbType.Float,
            DbType.Time => MySqlDbType.Time,
            DbType.UInt16 => MySqlDbType.UInt16,
            DbType.UInt32 => MySqlDbType.UInt32,
            DbType.UInt64 => MySqlDbType.UInt64,
            DbType.VarNumeric => MySqlDbType.Decimal,
            _ => throw new Exception($"this db type {columnDefinition.GetDbType()} for column {columnDefinition.ColumnName} is not supported"),
        };

        /// <inheritdoc/>
        public override Type GetType(SyncColumn columnDefinition) => this.GetMySqlDbType(columnDefinition) switch
        {
            MySqlDbType.Decimal or MySqlDbType.NewDecimal => typeof(decimal),
            MySqlDbType.Byte => typeof(byte),
            MySqlDbType.UByte => typeof(byte),
            MySqlDbType.Int16 or MySqlDbType.Year => typeof(short),
            MySqlDbType.Int24 or MySqlDbType.Int32 => typeof(int),
            MySqlDbType.UInt16 => typeof(ushort),
            MySqlDbType.Int64 => typeof(long),
            MySqlDbType.UInt24 or MySqlDbType.UInt32 => typeof(uint),
            MySqlDbType.Bit or MySqlDbType.Bool => typeof(bool),
            MySqlDbType.UInt64 => typeof(ulong),
            MySqlDbType.Float => typeof(float),
            MySqlDbType.Double => typeof(double),
            MySqlDbType.Time => typeof(TimeSpan),
            MySqlDbType.Date or MySqlDbType.DateTime or MySqlDbType.Newdate or MySqlDbType.Timestamp => typeof(DateTime),
            MySqlDbType.Enum or MySqlDbType.VarString or MySqlDbType.JSON
            or MySqlDbType.VarChar or MySqlDbType.String or MySqlDbType.TinyText or MySqlDbType.MediumText or MySqlDbType.LongText or MySqlDbType.Text or MySqlDbType.Set => typeof(string),
            MySqlDbType.Guid => typeof(Guid),
            MySqlDbType.TinyBlob or MySqlDbType.MediumBlob or MySqlDbType.LongBlob or MySqlDbType.Blob
            or MySqlDbType.Geometry or MySqlDbType.VarBinary or MySqlDbType.Binary => typeof(byte[]),
            _ => throw new Exception($"In Column {columnDefinition.ColumnName}, the type {this.GetMySqlDbType(columnDefinition)} is not supported"),
        };

        /// <inheritdoc/>
        public override int GetMaxLength(SyncColumn columnDefinition)
        {
            // blob
            if (string.Equals(columnDefinition.OriginalTypeName, "longblob", SyncGlobalization.DataSourceStringComparison)
                || string.Equals(columnDefinition.OriginalTypeName, "mediumblob", SyncGlobalization.DataSourceStringComparison)
                || string.Equals(columnDefinition.OriginalTypeName, "tinyblob", SyncGlobalization.DataSourceStringComparison))
            {
                return 0;
            }

            // text
            if (string.Equals(columnDefinition.OriginalTypeName, "longtext", SyncGlobalization.DataSourceStringComparison)
                || string.Equals(columnDefinition.OriginalTypeName, "mediumtext", SyncGlobalization.DataSourceStringComparison)
                || string.Equals(columnDefinition.OriginalTypeName, "tinytext", SyncGlobalization.DataSourceStringComparison)
                || string.Equals(columnDefinition.OriginalTypeName, "json", SyncGlobalization.DataSourceStringComparison))
            {
                return 0;
            }

            var iMaxLength = columnDefinition.MaxLength > 8000 ? 8000 : Convert.ToInt32(columnDefinition.MaxLength);
            return iMaxLength;
        }

        /// <inheritdoc/>
        public override (byte Precision, byte Scale) GetPrecisionAndScale(SyncColumn columnDefinition)
        {
            var precision = columnDefinition.Precision;
            var scale = columnDefinition.Scale;

            if ((columnDefinition.DbType == (int)DbType.Single || columnDefinition.DbType == (int)DbType.Decimal || columnDefinition.DbType == (int)DbType.VarNumeric) && columnDefinition.Precision == 0 && columnDefinition.Scale == 0)
                return (PRECISIONDEFAULT, SCALEDEFAULT);

            if (this.IsNumericType(columnDefinition) && precision == 0)
                return (PRECISIONDEFAULT, 0);

            if (!this.IsSupportingScale(columnDefinition) || scale == 0)
                return (0, 0);

            return CoercePrecisionAndScale(precision, scale);
        }

        /// <inheritdoc/>
        public override byte GetPrecision(SyncColumn columnDefinition)
        {
            var (p, _) = this.GetPrecisionAndScale(columnDefinition);
            return p;
        }

        /// <inheritdoc/>
        public override bool IsSupportingScale(SyncColumn columnDefinition) => columnDefinition.OriginalTypeName.ToLowerInvariant() switch
        {
            "numeric" or "decimal" or "dec" or "real" => true,
            _ => false,
        };

        /// <inheritdoc/>
        public override bool IsNumericType(SyncColumn columnDefinition) => columnDefinition.OriginalTypeName.ToLowerInvariant() switch
        {
            "int" or "int16" or "int24" or "int32" or "int64" or "uint16" or "uint24" or "uint32" or "uint64" or "integer" or
            "numeric" or "decimal" or "dec" or "fixed" or "tinyint" or "mediumint" or "bigint" or "real" or "double" or
            "float" or "serial" or "smallint" => true,
            _ => false,
        };

        /// <inheritdoc/>
        public override bool IsValid(SyncColumn columnDefinition) => columnDefinition.OriginalTypeName.ToLowerInvariant() switch
        {
            "int" or "int16" or "int24" or "int32" or "int64" or "uint16" or "uint24" or "uint32" or "uint64" or
            "bit" or "integer" or "datetime" or "date" or "newdate" or "numeric" or "decimal" or "dec" or "fixed" or
            "tinyint" or "mediumint" or "bigint" or "real" or "double" or "float" or "serial" or "smallint" or
            "varchar" or "char" or "text" or "longtext" or "tinytext" or "mediumtext" or "nchar" or "nvarchar" or
            "enum" or "set" or "blob" or "longblob" or "tinyblob" or "mediumblob" or "binary" or "varbinary" or
            "year" or "time" or "timestamp" or "json" => true,
            _ => false,
        };

        /// <inheritdoc/>
        public override bool IsReadonly(SyncColumn columnDefinition) => columnDefinition.IsCompute;

        /// <summary>
        /// Check compatibility.
        /// </summary>
        public bool IsCompatibleTextType(SyncColumn column, string fromProviderType)
        {
#if MARIADB
            var originalProvider = MariaDBSyncProvider.ProviderType;
#elif MYSQL
            var originalProvider = MySqlSyncProvider.ProviderType;
#endif
            if (fromProviderType == originalProvider)
            {
                return column.OriginalTypeName.ToLowerInvariant() switch
                {
                    "varbinary" or "binary" or "varchar" or "char" or "text" or "longtext" or "tinytext" or
                    "mediumtext" or "nchar" or "nvarchar" or "enum" or "set" or "json" => true,
                    _ => false,
                };
            }
            else
            {
                var dbType = (DbType)column.DbType;

                return dbType switch
                {
                    DbType.AnsiString or DbType.AnsiStringFixedLength or DbType.Guid or DbType.String or
                    DbType.StringFixedLength or DbType.Xml => true,
                    _ => false,
                };
            }
        }

        /// <summary>
        /// Gets a compatible column definition, like nvarchar(50), int, decimal(8,2).
        /// </summary>
        public string GetCompatibleColumnTypeDeclarationString(SyncColumn column, string fromProviderType)
        {

#if MARIADB
            var originalProvider = MariaDBSyncProvider.ProviderType;
#elif MYSQL
            var originalProvider = MySqlSyncProvider.ProviderType;
#endif
            if (fromProviderType == originalProvider && !string.IsNullOrEmpty(column.ExtraProperty1))
                return column.ExtraProperty1;

            // Fallback on my sql db type extract from simple db type
            var typeName = this.GetCompatibleStringFromDbType((DbType)column.DbType, column.MaxLength).ToString();

            var argument = this.GetCompatiblePrecisionStringFromDbType(column, fromProviderType);

            return string.IsNullOrEmpty(argument) ? typeName : $"{typeName} {argument}";
        }

        /// <summary>
        /// Get string from owner db type.
        /// </summary>
        public string GetStringFromOwnerDbType(MySqlDbType ownerType) => ownerType switch
        {
            MySqlDbType.Decimal or MySqlDbType.NewDecimal => "decimal",
            MySqlDbType.Bool => "bit",
            MySqlDbType.Byte or MySqlDbType.UByte => "tinyint",
            MySqlDbType.Int16 or MySqlDbType.Year or MySqlDbType.UInt16 => "smallint",
            MySqlDbType.Int24 or MySqlDbType.UInt24 => "mediumint",
            MySqlDbType.Int32 or MySqlDbType.UInt32 => "int",
            MySqlDbType.Int64 or MySqlDbType.UInt64 => "bigint",
            MySqlDbType.Bit => "bit",
            MySqlDbType.Float => "float",
            MySqlDbType.Double => "double",
            MySqlDbType.Time => "time",
            MySqlDbType.Date => "date",
            MySqlDbType.DateTime => "datetime",
            MySqlDbType.Newdate => "newdate",
            MySqlDbType.Enum => "enum",
            MySqlDbType.TinyText => "tinytext",
            MySqlDbType.MediumText => "mediumtext",
            MySqlDbType.LongText => "longtext",
            MySqlDbType.Text => "text",
            MySqlDbType.JSON => "JSON",
            MySqlDbType.VarChar or MySqlDbType.VarString => "varchar",
            MySqlDbType.String or MySqlDbType.Guid => "char",
            MySqlDbType.Set => "set",
            MySqlDbType.Timestamp => "timestamp",
            MySqlDbType.TinyBlob => "tinyblob",
            MySqlDbType.MediumBlob => "mediumblob",
            MySqlDbType.LongBlob => "longblob",
            MySqlDbType.Blob => "blob",
            MySqlDbType.VarBinary => "varbinary",
            MySqlDbType.Binary => "binary",
            MySqlDbType.Geometry => "geometry",
            _ => throw new Exception("Unhandled type encountered"),
        };

        /// <summary>
        /// Get compatible string from db type.
        /// </summary>
        public string GetCompatibleStringFromDbType(DbType dbType, int maxLength) => dbType switch
        {
            DbType.Binary => maxLength <= 0 || maxLength > 8000 ? "longblob" : "varbinary",
            DbType.Boolean => "bit",
            DbType.Byte or DbType.SByte => "tinyint",
            DbType.Time => "time",
            DbType.Date => "date",
            DbType.DateTime or DbType.DateTime2 or DbType.DateTimeOffset => "datetime",
            DbType.Currency or DbType.Decimal or DbType.Double or DbType.Single or DbType.VarNumeric => "decimal",
            DbType.Int16 or DbType.UInt16 => "smallint",
            DbType.Int32 or DbType.UInt32 => "int",
            DbType.Int64 or DbType.UInt64 => "bigint",
            DbType.String or DbType.AnsiString or DbType.Xml => "longtext",
            DbType.StringFixedLength or DbType.AnsiStringFixedLength => "varchar",
            DbType.Guid => "char",
            DbType.Object => "longblob",
            _ => throw new Exception($"sqltype not valid"),
        };

        /// <summary>
        /// Get compatible column precision and scale.
        /// </summary>
        public (byte Precision, byte Scale) GetCompatibleColumnPrecisionAndScale(SyncColumn column, string fromProviderType)
        {
#if MARIADB
            var originalProvider = MariaDBSyncProvider.ProviderType;
#elif MYSQL
            var originalProvider = MySqlSyncProvider.ProviderType;
#endif

            // We get the sql db type from the original provider otherwise fallback on sql db type extract from simple db type
            // var mySqlDbType = fromProviderType == originalProvider ?
            //    this.GetMySqlDbType(column) : this.GetOwnerDbTypeFromDbType(column);
            return this.GetPrecisionAndScale(column);
        }

        /// <summary>
        /// Get Compatible max length.
        /// </summary>
        public int GetCompatibleMaxLength(SyncColumn column, string fromProviderType)
        {
#if MARIADB
            var originalProvider = MariaDBSyncProvider.ProviderType;
#elif MYSQL
            var originalProvider = MySqlSyncProvider.ProviderType;
#endif

            var mySqlDbType = fromProviderType == originalProvider ?
                this.GetMySqlDbType(column) : this.GetOwnerDbTypeFromDbType(column);

            return mySqlDbType switch
            {
                MySqlDbType.VarBinary => column.MaxLength > 0 ? Math.Min(column.MaxLength, 8000) : 8000,
                MySqlDbType.Binary => column.MaxLength > 0 ? Math.Min(column.MaxLength, 255) : 255,
                MySqlDbType.VarChar or MySqlDbType.String or MySqlDbType.Text or MySqlDbType.Enum or MySqlDbType.Set => Math.Max(0, column.MaxLength),
                _ => 0,
            };
        }

        /// <summary>
        /// Get compatible precision from db type.
        /// </summary>
        public string GetCompatiblePrecisionStringFromDbType(SyncColumn column, string fromProviderType)
        {
#if MARIADB
            var originalProvider = MariaDBSyncProvider.ProviderType;
#elif MYSQL
            var originalProvider = MySqlSyncProvider.ProviderType;
#endif
            if (column.GetDbType() == DbType.Guid)
                return "(36)";

            var mySqlDbType = fromProviderType == originalProvider ?
                this.GetMySqlDbType(column) : this.GetOwnerDbTypeFromDbType(column);

            var (precision, scale) = this.GetPrecisionAndScale(column);

            return mySqlDbType switch
            {
                MySqlDbType.Float or
                MySqlDbType.Decimal or
                MySqlDbType.Double => string.Format("({0},{1})", precision, scale),
                MySqlDbType.VarChar or MySqlDbType.Text or MySqlDbType.Enum or MySqlDbType.Set => column.MaxLength > 0 ? $"({column.MaxLength})" : string.Empty,
                MySqlDbType.Binary => column.MaxLength > 0 ? $"({Math.Min(column.MaxLength, 255)})" : "(255)",
                MySqlDbType.VarBinary => column.MaxLength > 0 ? $"({Math.Min(column.MaxLength, 8000)})" : "(8000)",
                _ => string.Empty,
            };
        }
    }
}