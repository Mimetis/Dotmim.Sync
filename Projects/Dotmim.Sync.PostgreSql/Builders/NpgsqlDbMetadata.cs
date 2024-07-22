using Dotmim.Sync.Manager;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.PostgreSql.Builders
{
    public class NpgsqlDbMetadata : DbMetadata
    {
        public const byte PRECISIONMAX = 28;
        public const byte SCALEMAX = 18;

        public NpgsqlDbMetadata() { }

        public static (byte p, byte s) CoercePrecisionAndScale(int precision, int scale)
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

        public (byte p, byte s) GetCompatibleColumnPrecisionAndScale(SyncColumn column, string fromProviderType)
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

        public override DbType GetDbType(SyncColumn columnDefinition)
        {
            switch (columnDefinition.OriginalTypeName.ToLowerInvariant())
            {
                case "smallint":
                case "int2":
                case "int2vector":
                case "smallserial":
                case "serial2":
                    return DbType.Int16;

                case "integer":
                case "int":
                case "int4":
                case "serial":
                case "serial4":
                    return DbType.Int32;

                case "bigint":
                case "int8":
                case "bigserial":
                case "serial8":
                    return DbType.Int64;

                // Bit strings are strings of 1's and 0's.They can be used to store or visualize bit masks.
                // https://www.postgresql.org/docs/current/datatype-bit.html
                case "bit":
                    return DbType.Boolean;
                case "varbit":
                case "bit varying":
                    return DbType.String;

                case "boolean":
                case "bool":
                    return DbType.Boolean;

                // IPv4, IPv6, and MAC addresses
                // https://www.postgresql.org/docs/current/datatype-net-types.html
                case "cid":
                case "cidr":
                case "inet":
                case "macaddr":
                case "macaddr8":
                    return DbType.String;

                // Full text search text
                // https://www.postgresql.org/docs/current/datatype-textsearch.html
                case "tsquery":
                case "tsvector":
                    return DbType.String;

                // Geometry
                case "geometry":
                case "box":
                case "circle":
                case "line":
                case "lseg":
                case "path":
                case "polygon":
                    return DbType.String;

                case "bytea":
                    return DbType.Binary;

                case "character":
                case "char":
                case "name":
                case "bpchar":
                    return DbType.AnsiStringFixedLength;

                case "varchar":
                case "character varying":
                case "refcursor":
                case "citext":
                case "text":
                    return DbType.AnsiString;

                case "date":
                    return DbType.Date;

                case "timestamp":
                case "timestamp without time zone":
                    return DbType.DateTime2;

                case "timestamptz":
                case "timestamp with time zone":
                case "timetz":
                case "time with time zone":
                    return DbType.DateTimeOffset;

                case "time":
                case "time without time zone":
                    return DbType.Time;

                case "double precision":
                case "float8":
                    return DbType.Double;

                // https://www.postgresql.org/docs/current/hstore.html
                case "hstore":
                    return DbType.String;

                case "json":
                case "jsonb":
                    return DbType.String;

                case "money":
                    return DbType.Currency;

                case "numeric":
                    return DbType.VarNumeric;

                case "float4":
                case "real":
                    return DbType.Decimal;

                case "uuid":
                    return DbType.Guid;

                case "xml":
                    return DbType.String;
                case "array":
                    return DbType.Object;
            }

            throw new Exception($"this type {columnDefinition.OriginalTypeName.ToLowerInvariant()} is not supported");
        }

        public override int GetMaxLength(SyncColumn columnDefinition)
        {
            var sqlDbType = this.GetNpgsqlDbType(columnDefinition);

            var iMaxLength = columnDefinition.MaxLength > 8000 ? 8000 : Convert.ToInt32(columnDefinition.MaxLength);

            if (iMaxLength > 0 && sqlDbType != NpgsqlDbType.Varchar && sqlDbType != NpgsqlDbType.Text &&
                sqlDbType != NpgsqlDbType.Char && sqlDbType != NpgsqlDbType.Bytea)
                iMaxLength = 0;

            return iMaxLength;
        }

        public NpgsqlDbType GetNpgsqlDbType(SyncColumn column) => (NpgsqlDbType)this.GetOwnerDbType(column);

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

        public override byte GetPrecision(SyncColumn columnDefinition)
        {
            var (p, _) = CoercePrecisionAndScale(columnDefinition.Precision, columnDefinition.Scale);

            return p;
        }

        public override (byte Precision, byte Scale) GetPrecisionAndScale(SyncColumn columnDefinition)
        {
            if (columnDefinition.DbType == (int)DbType.Single && columnDefinition.Precision == 0 && columnDefinition.Scale == 0)
                return (PRECISIONMAX, 8);

            return CoercePrecisionAndScale(columnDefinition.Precision, columnDefinition.Scale);
        }

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

        public override bool IsNumericType(SyncColumn columnDefinition)
        {
            switch (columnDefinition.OriginalTypeName.ToLowerInvariant())
            {
                case "bigint":
                case "int8":
                case "bigserial":
                case "serial8":
                case "double precision":
                case "float8":
                case "integer":
                case "int":
                case "int4":
                case "numeric":
                case "decimal":
                case "real":
                case "float4":
                case "smallint":
                case "int2":
                case "smallserial":
                case "serial2":
                case "serial":
                case "serial4":

                    return true;
            }

            return false;
        }

        public override bool IsReadonly(SyncColumn columnDefinition) => string.Equals(columnDefinition.OriginalTypeName, "timestamp", SyncGlobalization.DataSourceStringComparison) || columnDefinition.IsCompute;

        public override bool IsSupportingScale(SyncColumn columnDefinition)
        {
            switch (columnDefinition.OriginalTypeName.ToLowerInvariant())
            {
                case "real":
                case "money":
                case "numeric":
                    return true;
            }

            return false;
        }

        public override bool IsValid(SyncColumn columnDefinition)
        {
            switch (columnDefinition.OriginalTypeName.ToLowerInvariant())
            {
                case "array":
                case "smallint":
                case "int2":
                case "int2vector":
                case "smallserial":
                case "serial2":
                case "integer":
                case "int":
                case "int4":
                case "serial":
                case "serial4":
                case "bigint":
                case "int8":
                case "bigserial":
                case "serial8":
                case "bit":
                case "varbit":
                case "bit varying":
                case "boolean":
                case "bool":
                case "cid":
                case "cidr":
                case "inet":
                case "macaddr":
                case "macaddr8":
                case "tsquery":
                case "tsvector":
                case "geometry":
                case "box":
                case "circle":
                case "line":
                case "lseg":
                case "path":
                case "polygon":
                case "bytea":
                case "character":
                case "char":
                case "name":
                case "character varying":
                case "varchar":
                case "refcursor":
                case "citext":
                case "text":
                case "date":
                case "timestamp":
                case "timestamp without time zone":
                case "timestamptz":
                case "timestamp with time zone":
                case "timetz":
                case "time with time zone":
                case "time":
                case "time without time zone":
                case "double precision":
                case "float8":
                case "hstore":
                case "json":
                case "jsonb":
                case "money":
                case "numeric":
                case "float4":
                case "real":
                case "uuid":
                case "xml":
                case "bpchar":
                    return true;
            }

            return false;
        }
    }
}