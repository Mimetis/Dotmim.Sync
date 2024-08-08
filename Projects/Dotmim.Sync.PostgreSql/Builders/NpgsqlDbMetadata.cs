using Dotmim.Sync.Manager;
using NpgsqlTypes;
using System;
using System.Data;

namespace Dotmim.Sync.PostgreSql.Builders
{
    /// <summary>
    /// Represents a PostgreSQL database metadata.
    /// </summary>
    public class NpgsqlDbMetadata : DbMetadata
    {
        /// <summary>
        /// Represents the maximum precision for a numeric column.
        /// </summary>
        public const byte PRECISIONMAX = 28;

        /// <summary>
        /// Represents the maximum scale for a numeric column.
        /// </summary>
        public const byte SCALEMAX = 18;

        /// <summary>
        /// Initializes a new instance of the <see cref="NpgsqlDbMetadata"/> class.
        /// </summary>
        public NpgsqlDbMetadata() { }

        /// <summary>
        /// Coerces the precision and scale of a column.
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

        /// <summary>
        /// Gets the NpgsqlDbType from the column definition.
        /// </summary>
        public static NpgsqlDbType GetOwnerDbTypeFromDbType(SyncColumn column)
        {
#if NET6_0_OR_GREATER
            // Getting EnableLegacyTimestampBehavior behavior
            var legacyTimestampBehavior = false;
            AppContext.TryGetSwitch("Npgsql.EnableLegacyTimestampBehavior", out legacyTimestampBehavior);
#else
            var legacyTimestampBehavior = true;
#endif
            var npgsqlDbType = column.GetDbType() switch
            {
                DbType.AnsiStringFixedLength or DbType.AnsiString or DbType.String or DbType.StringFixedLength => NpgsqlDbType.Varchar,
                DbType.Binary => NpgsqlDbType.Bytea,
                DbType.Boolean => NpgsqlDbType.Boolean,
                DbType.Byte or DbType.SByte => NpgsqlDbType.Smallint,
                DbType.Currency => NpgsqlDbType.Money,
                DbType.Date => NpgsqlDbType.Date,
                DbType.Time => NpgsqlDbType.Time,
                DbType.DateTime2 => NpgsqlDbType.Timestamp,
                DbType.DateTime => legacyTimestampBehavior ? NpgsqlDbType.Timestamp : NpgsqlDbType.TimestampTz,
                DbType.DateTimeOffset => NpgsqlDbType.TimestampTz,
                DbType.Single => NpgsqlDbType.Real,
                DbType.Decimal or DbType.VarNumeric => NpgsqlDbType.Numeric,
                DbType.Double => NpgsqlDbType.Double,
                DbType.Guid => NpgsqlDbType.Uuid,
                DbType.Int16 or DbType.UInt16 => NpgsqlDbType.Smallint,
                DbType.Int32 or DbType.UInt32 => NpgsqlDbType.Integer,
                DbType.Int64 or DbType.UInt64 => NpgsqlDbType.Bigint,
                DbType.Xml => NpgsqlDbType.Text,
                _ => throw new Exception($"this type name {column.GetType()} is not supported"),
            };

            if (npgsqlDbType == NpgsqlDbType.Varchar && column.MaxLength <= 0)
                npgsqlDbType = NpgsqlDbType.Text;

            return npgsqlDbType;
        }

        /// <summary>
        /// Gets the compatible column precision and scale.
        /// </summary>
        public (byte Precision, byte Scale) GetCompatibleColumnPrecisionAndScale(SyncColumn column, string fromProviderType)
        {
            // We get the sql db type from the original provider otherwise fallback on sql db type extract from simple db type
            var sqlDbType = fromProviderType == NpgsqlSyncProvider.ProviderType ?
                this.GetNpgsqlDbType(column) : GetOwnerDbTypeFromDbType(column);

            return sqlDbType switch
            {
                NpgsqlDbType.Numeric => CoercePrecisionAndScale(column.Precision, column.Scale),
                _ => (0, 0),
            };
        }

        /// <summary>
        /// Gets the compatible column type declaration string.
        /// </summary>
        public string GetCompatibleColumnTypeDeclarationString(SyncColumn column, string fromProviderType)
        {
            string argument = string.Empty;

            // We get the sql db type from the original provider otherwise fallback on sql db type extract from simple db type
            var sqlDbType = fromProviderType == NpgsqlSyncProvider.ProviderType ? this.GetNpgsqlDbType(column) : GetOwnerDbTypeFromDbType(column);

            switch (sqlDbType)
            {
                case NpgsqlDbType.Varbit:
                case NpgsqlDbType.Varchar:
                case NpgsqlDbType.Char:
                    // case NpgsqlDbType.Text:
                    argument = column.MaxLength > 0 ? $"({column.MaxLength})" : string.Empty;
                    break;
                case NpgsqlDbType.Numeric:
                    var (p, s) = this.GetPrecisionAndScale(column);

                    if (column.DbType == (int)DbType.Single && column.Precision == 0 && column.Scale == 0)
                        argument = $"({PRECISIONMAX}, 8)";
                    else if (p > 0 && s <= 0)
                        argument = $"({p})";
                    else if (p > 0 && s > 0)
                        argument = $"({p}, {s})";
                    break;
                default:
                    argument = string.Empty;
                    break;
            }

            string typeName = fromProviderType == NpgsqlSyncProvider.ProviderType ? column.OriginalTypeName.ToLowerInvariant() : sqlDbType.ToString().ToLowerInvariant();

            return string.IsNullOrEmpty(argument) ? typeName : $"{typeName} {argument}";
        }

        /// <summary>
        /// Gets the compatible max length.
        /// </summary>
        public int GetCompatibleMaxLength(SyncColumn column, string fromProviderType)
        {
            // We get the sql db type from the original provider otherwise fallback on sql db type extract from simple db type
            var sqlDbType = fromProviderType == NpgsqlSyncProvider.ProviderType ?
                this.GetNpgsqlDbType(column) : GetOwnerDbTypeFromDbType(column);

            return sqlDbType switch
            {
                NpgsqlDbType.Varbit or NpgsqlDbType.Char or NpgsqlDbType.Varchar => column.MaxLength,
                _ => 0,
            };
        }

        /// <inheritdoc/>
        public override DbType GetDbType(SyncColumn columnDefinition) => columnDefinition.OriginalTypeName.ToLowerInvariant() switch
        {
            "smallint" or "int2" or "int2vector" or "smallserial" or "serial2" => DbType.Int16,
            "integer" or "int" or "int4" or "serial" or "serial4" => DbType.Int32,
            "bigint" or "int8" or "bigserial" or "serial8" => DbType.Int64,

            // Bit strings are strings of 1's and 0's.They can be used to store or visualize bit masks.
            // https://www.postgresql.org/docs/current/datatype-bit.html
            "bit" => DbType.Boolean,
            "varbit" or "bit varying" => DbType.String,
            "boolean" or "bool" => DbType.Boolean,

            // IPv4, IPv6, and MAC addresses
            // https://www.postgresql.org/docs/current/datatype-net-types.html
            "cid" or "cidr" or "inet" or "macaddr" or "macaddr8" => DbType.String,

            // Full text search text
            // https://www.postgresql.org/docs/current/datatype-textsearch.html
            "tsquery" or "tsvector" => DbType.String,

            // Geometry
            "geometry" or "box" or "circle" or "line" or "lseg" or "path" or "polygon" => DbType.String,
            "bytea" => DbType.Binary,
            "character" or "char" or "name" or "bpchar" => DbType.AnsiStringFixedLength,
            "varchar" or "character varying" or "refcursor" or "citext" or "text" => DbType.AnsiString,
            "date" => DbType.Date,
            "timestamp" or "timestamp without time zone" => DbType.DateTime2,
            "timestamptz" or "timestamp with time zone" or "timetz" or "time with time zone" => DbType.DateTimeOffset,
            "time" or "time without time zone" => DbType.Time,
            "double precision" or "float8" => DbType.Double,

            // https://www.postgresql.org/docs/current/hstore.html
            "hstore" => DbType.String,
            "json" or "jsonb" => DbType.String,
            "money" => DbType.Currency,
            "numeric" => DbType.VarNumeric,
            "float4" or "real" => DbType.Decimal,
            "uuid" => DbType.Guid,
            "xml" => DbType.String,
            "array" => DbType.Object,
            _ => throw new Exception($"this type {columnDefinition.OriginalTypeName.ToLowerInvariant()} is not supported"),
        };

        /// <inheritdoc/>
        public override int GetMaxLength(SyncColumn columnDefinition)
        {
            var sqlDbType = this.GetNpgsqlDbType(columnDefinition);

            var iMaxLength = columnDefinition.MaxLength > 8000 ? 8000 : Convert.ToInt32(columnDefinition.MaxLength);

            if (iMaxLength > 0 && sqlDbType != NpgsqlDbType.Varchar && sqlDbType != NpgsqlDbType.Text &&
                sqlDbType != NpgsqlDbType.Char && sqlDbType != NpgsqlDbType.Bytea)
                iMaxLength = 0;

            return iMaxLength;
        }

        /// <summary>
        /// Gets the NpgsqlDbType from the column definition.
        /// </summary>
        public NpgsqlDbType GetNpgsqlDbType(SyncColumn column) => (NpgsqlDbType)this.GetOwnerDbType(column);

        /// <inheritdoc/>
        public override object GetOwnerDbType(SyncColumn columnDefinition) => columnDefinition.OriginalTypeName.ToLowerInvariant() switch
        {
            "bigint" or "int8" => NpgsqlDbType.Bigint,
            "bit" => NpgsqlDbType.Bit,
            "boolean" or "bool" => NpgsqlDbType.Boolean,
            "box" => NpgsqlDbType.Box,
            "bytea" => NpgsqlDbType.Bytea,
            "character" or "char" or "bpchar" => NpgsqlDbType.Char,
            "cid" => NpgsqlDbType.Cid,
            "cidr" => NpgsqlDbType.Cidr,
            "circle" => NpgsqlDbType.Circle,
            "citext" => NpgsqlDbType.Citext,
            "date" => NpgsqlDbType.Date,
            "double precision" or "float8" => NpgsqlDbType.Double,
            "geography" => NpgsqlDbType.Geography,
            "geometry" => NpgsqlDbType.Geometry,
            "hstore" => NpgsqlDbType.Hstore,
            "inet" => NpgsqlDbType.Inet,
            "int2vector" => NpgsqlDbType.Int2Vector,
            "integer" or "int" or "int2" or "int4" => NpgsqlDbType.Integer,
            "internalchar" => NpgsqlDbType.InternalChar,
            "interval" => NpgsqlDbType.Interval,
            "json" => NpgsqlDbType.Json,
            "jsonb" => NpgsqlDbType.Jsonb,
            "line" => NpgsqlDbType.Line,
            "lseg" => NpgsqlDbType.LSeg,
            "macaddr" => NpgsqlDbType.MacAddr,
            "macaddr8" => NpgsqlDbType.MacAddr8,
            "money" => NpgsqlDbType.Money,
            "name" => NpgsqlDbType.Name,
            "decimal" or "numeric" => NpgsqlDbType.Numeric,
            "oid" => NpgsqlDbType.Oid,
            "oidvector" => NpgsqlDbType.Oidvector,
            "path" => NpgsqlDbType.Path,
            "point" => NpgsqlDbType.Point,
            "polygon" => NpgsqlDbType.Polygon,
            "range" => NpgsqlDbType.Range,
            "real" or "float4" => NpgsqlDbType.Real,
            "refcursor" => NpgsqlDbType.Refcursor,
            "regconfig" => NpgsqlDbType.Regconfig,
            "regtype" => NpgsqlDbType.Regtype,
            "smallint" => NpgsqlDbType.Smallint,
            "text" => NpgsqlDbType.Text,
            "tid" => NpgsqlDbType.Tid,
            "time" or "time without time zone" => NpgsqlDbType.Time,
            "timestamp" or "timestamp without time zone" => NpgsqlDbType.Timestamp,
            "timestamptz" or "timestamp with time zone" => NpgsqlDbType.TimestampTz,
            "timetz" or "time with time zone" => NpgsqlDbType.TimeTz,
            "tsquery" => NpgsqlDbType.TsQuery,
            "tsvector" => NpgsqlDbType.TsVector,
            "uuid" => NpgsqlDbType.Uuid,
            "varbit" => NpgsqlDbType.Varbit,
            "varchar" or "character varying" => NpgsqlDbType.Varchar,
            "xid" => NpgsqlDbType.Xid,
            "xml" => (object)NpgsqlDbType.Xml,
            _ => throw new Exception($"Type '{columnDefinition.OriginalTypeName.ToLowerInvariant()}' (column {columnDefinition.ColumnName}) is not supported"),
        };

        /// <inheritdoc/>
        public override byte GetPrecision(SyncColumn columnDefinition)
        {
            var (p, _) = CoercePrecisionAndScale(columnDefinition.Precision, columnDefinition.Scale);

            return p;
        }

        /// <inheritdoc/>
        public override (byte Precision, byte Scale) GetPrecisionAndScale(SyncColumn columnDefinition)
        {
            return columnDefinition.DbType == (int)DbType.Single && columnDefinition.Precision == 0 && columnDefinition.Scale == 0
                ? ((byte Precision, byte Scale))(PRECISIONMAX, 8)
                : CoercePrecisionAndScale(columnDefinition.Precision, columnDefinition.Scale);
        }

        /// <inheritdoc/>
        public override Type GetType(SyncColumn columnDefinition) => this.GetNpgsqlDbType(columnDefinition) switch
        {
            NpgsqlDbType.Bigint => typeof(long),
            NpgsqlDbType.Double => typeof(double),
            NpgsqlDbType.Int2Vector or NpgsqlDbType.Integer => typeof(int),
            NpgsqlDbType.Real => typeof(float),
            NpgsqlDbType.Numeric or NpgsqlDbType.Money => typeof(decimal),
            NpgsqlDbType.Smallint => typeof(short),
            NpgsqlDbType.Boolean => typeof(bool),
            NpgsqlDbType.Char => typeof(char),
            NpgsqlDbType.Text or NpgsqlDbType.Varchar or NpgsqlDbType.Name or NpgsqlDbType.Citext => typeof(string),
            NpgsqlDbType.Bytea => typeof(byte[]),
            NpgsqlDbType.Date or NpgsqlDbType.Timestamp => typeof(DateTime),
            NpgsqlDbType.Time => typeof(TimeSpan),
            NpgsqlDbType.TimestampTz or NpgsqlDbType.TimeTz => typeof(DateTimeOffset),
            NpgsqlDbType.Inet or NpgsqlDbType.Cidr or NpgsqlDbType.MacAddr or NpgsqlDbType.MacAddr8 or NpgsqlDbType.Bit or NpgsqlDbType.Varbit or NpgsqlDbType.TsVector or NpgsqlDbType.TsQuery => typeof(string),
            NpgsqlDbType.Uuid => typeof(Guid),
            NpgsqlDbType.Xml or NpgsqlDbType.Json or NpgsqlDbType.Jsonb or NpgsqlDbType.Hstore => typeof(string),
            _ => throw new Exception($"this NpgsqlDbType {this.GetNpgsqlDbType(columnDefinition)} is not supported"),
        };

        /// <inheritdoc/>
        public override bool IsNumericType(SyncColumn columnDefinition) => columnDefinition.OriginalTypeName.ToLowerInvariant() switch
        {
            "bigint" or "int8" or "bigserial" or "serial8" or "double precision" or "float8" or "integer" or "int" or "int4" or "numeric" or "decimal" or "real" or "float4" or "smallint" or "int2" or "smallserial" or "serial2" or "serial" or "serial4" => true,
            _ => false,
        };

        /// <inheritdoc/>
        public override bool IsReadonly(SyncColumn columnDefinition) => string.Equals(columnDefinition.OriginalTypeName, "timestamp", SyncGlobalization.DataSourceStringComparison) || columnDefinition.IsCompute;

        /// <inheritdoc/>
        public override bool IsSupportingScale(SyncColumn columnDefinition) => columnDefinition.OriginalTypeName.ToLowerInvariant() switch
        {
            "real" or "money" or "numeric" => true,
            _ => false,
        };

        /// <inheritdoc/>
        public override bool IsValid(SyncColumn columnDefinition) => columnDefinition.OriginalTypeName.ToLowerInvariant() switch
        {
            "array" or "smallint" or "int2" or "int2vector" or "smallserial" or "serial2" or "integer" or "int" or "int4" or "serial" or "serial4" or "bigint" or "int8" or "bigserial" or "serial8" or "bit" or "varbit" or "bit varying" or "boolean" or "bool" or "cid" or "cidr" or "inet" or "macaddr" or "macaddr8" or "tsquery" or "tsvector" or "geometry" or "box" or "circle" or "line" or "lseg" or "path" or "polygon" or "bytea" or "character" or "char" or "name" or "character varying" or "varchar" or "refcursor" or "citext" or "text" or "date" or "timestamp" or "timestamp without time zone" or "timestamptz" or "timestamp with time zone" or "timetz" or "time with time zone" or "time" or "time without time zone" or "double precision" or "float8" or "hstore" or "json" or "jsonb" or "money" or "numeric" or "float4" or "real" or "uuid" or "xml" or "bpchar" => true,
            _ => false,
        };
    }
}