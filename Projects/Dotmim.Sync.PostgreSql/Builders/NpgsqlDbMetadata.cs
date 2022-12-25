using Dotmim.Sync.Manager;
using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Linq;
using NpgsqlTypes;

namespace Dotmim.Sync.PostgreSql.Builders
{
    public class NpgsqlDbMetadata : DbMetadata
    {
        public const byte PRECISION_MAX = 28;
        public const byte SCALE_MAX = 18;
        public NpgsqlDbMetadata() { }
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

        public (byte p, byte s) GetCompatibleColumnPrecisionAndScale(SyncColumn column, string fromProviderType)
        {
            // We get the sql db type from the original provider otherwise fallback on sql db type extract from simple db type
            var sqlDbType = fromProviderType == NpgsqlSyncProvider.ProviderType ?
                this.GetNpgsqlDbType(column) : this.GetOwnerDbTypeFromDbType(column);

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
            var sqlDbType = fromProviderType == NpgsqlSyncProvider.ProviderType ? this.GetNpgsqlDbType(column) : this.GetOwnerDbTypeFromDbType(column);

            switch (sqlDbType)
            {
                case NpgsqlDbType.Varbit:
                case NpgsqlDbType.Varchar:
                case NpgsqlDbType.Char:
                    //case NpgsqlDbType.Text:
                    argument = $"({column.MaxLength})";
                    break;
                case NpgsqlDbType.Numeric:
                    var (p, s) = this.GetPrecisionAndScale(column);

                    if (column.DbType == (int)DbType.Single && column.Precision == 0 && column.Scale == 0)
                        argument = $"({PRECISION_MAX}, 8)";
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
                this.GetNpgsqlDbType(column) : this.GetOwnerDbTypeFromDbType(column);

            return sqlDbType switch
            {
                NpgsqlDbType.Varbit or NpgsqlDbType.Char or NpgsqlDbType.Varchar => column.MaxLength,
                _ => 0,
            };
        }

        public override DbType GetDbType(SyncColumn column)
        {
            switch (column.OriginalTypeName.ToLowerInvariant())
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
                case "character varying":
                    return DbType.AnsiStringFixedLength;


                case "varchar":
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
            throw new Exception($"this type {column.OriginalTypeName.ToLowerInvariant()} is not supported");
        }
        public override int GetMaxLength(SyncColumn column)
        {
            var sqlDbType = GetNpgsqlDbType(column);

            var iMaxLength = column.MaxLength > 8000 ? 8000 : Convert.ToInt32(column.MaxLength);

            //// special length for nchar and nvarchar
            //if ((sqlDbType == NpgsqlDbType.Bytea || sqlDbType == NpgsqlDbType.Varchar || sqlDbType == NpgsqlDbType.Text) && iMaxLength > 0)
            //    iMaxLength /= 2;

            if (iMaxLength > 0 && sqlDbType != NpgsqlDbType.Varchar && sqlDbType != NpgsqlDbType.Text &&
                sqlDbType != NpgsqlDbType.Char && sqlDbType != NpgsqlDbType.Bytea)
                iMaxLength = 0;

            return iMaxLength;
        }

        public NpgsqlDbType GetNpgsqlDbType(SyncColumn column) => (NpgsqlDbType)this.GetOwnerDbType(column);

        public override object GetOwnerDbType(SyncColumn column)
        {
            switch (column.OriginalTypeName.ToLowerInvariant())
            {
                case "array":
                    return NpgsqlDbType.Array;
                case "bigint":
                case "int8":
                    return NpgsqlDbType.Bigint;
                case "bit":
                    return NpgsqlDbType.Bit;
                case "boolean":
                case "bool":
                    return NpgsqlDbType.Boolean;
                case "box":
                    return NpgsqlDbType.Box;
                case "bytea":
                    return NpgsqlDbType.Bytea;
                case "character":
                case "char":
                case "bpchar":
                    return NpgsqlDbType.Char;
                case "cid":
                    return NpgsqlDbType.Cid;
                case "cidr":
                    return NpgsqlDbType.Cidr;
                case "circle":
                    return NpgsqlDbType.Circle;
                case "citext":
                    return NpgsqlDbType.Citext;
                case "date":
                    return NpgsqlDbType.Date;
                case "double precision":
                case "float8":
                    return NpgsqlDbType.Double;
                case "geography":
                    return NpgsqlDbType.Geography;
                case "geometry":
                    return NpgsqlDbType.Geometry;
                case "hstore":
                    return NpgsqlDbType.Hstore;
                case "inet":
                    return NpgsqlDbType.Inet;
                case "int2vector":
                    return NpgsqlDbType.Int2Vector;
                case "integer":
                case "int":
                case "int2":
                case "int4":
                    return NpgsqlDbType.Integer;
                case "internalchar":
                    return NpgsqlDbType.InternalChar;
                case "interval":
                    return NpgsqlDbType.Interval;
                case "json":
                    return NpgsqlDbType.Json;
                case "jsonb":
                    return NpgsqlDbType.Jsonb;
                case "line":
                    return NpgsqlDbType.Line;
                case "lseg":
                    return NpgsqlDbType.LSeg;
                case "macaddr":
                    return NpgsqlDbType.MacAddr;
                case "macaddr8":
                    return NpgsqlDbType.MacAddr8;
                case "money":
                    return NpgsqlDbType.Money;
                case "name":
                    return NpgsqlDbType.Name;
                case "decimal":
                case "numeric":
                    return NpgsqlDbType.Numeric;
                case "oid":
                    return NpgsqlDbType.Oid;
                case "oidvector":
                    return NpgsqlDbType.Oidvector;
                case "path":
                    return NpgsqlDbType.Path;
                case "point":
                    return NpgsqlDbType.Point;
                case "polygon":
                    return NpgsqlDbType.Polygon;
                case "range":
                    return NpgsqlDbType.Range;
                case "real":
                case "float4":
                    return NpgsqlDbType.Real;
                case "refcursor":
                    return NpgsqlDbType.Refcursor;
                case "regconfig":
                    return NpgsqlDbType.Regconfig;
                case "regtype":
                    return NpgsqlDbType.Regtype;
                case "smallint":
                    return NpgsqlDbType.Smallint;
                case "text":
                    return NpgsqlDbType.Text;
                case "tid":
                    return NpgsqlDbType.Tid;
                case "time":
                case "time without time zone":
                    return NpgsqlDbType.Time;
                case "timestamp":
                case "timestamp without time zone":
                    return NpgsqlDbType.Timestamp;
                case "timestamptz":
                case "timestamp with time zone":
                    return NpgsqlDbType.TimestampTz;
                case "timetz":
                case "time with time zone":
                    return NpgsqlDbType.TimeTz;
                case "tsquery":
                    return NpgsqlDbType.TsQuery;
                case "tsvector":
                    return NpgsqlDbType.TsVector;
                case "uuid":
                    return NpgsqlDbType.Uuid;
                case "varbit":
                    return NpgsqlDbType.Varbit;
                case "varchar":
                case "character varying":
                    return NpgsqlDbType.Varchar;
                case "xid":
                    return NpgsqlDbType.Xid;
                case "xml":
                    return NpgsqlDbType.Xml;
            }
            throw new Exception($"this type name {column.OriginalTypeName.ToLowerInvariant()} is not supported");
        }
        public NpgsqlDbType GetOwnerDbTypeFromDbType(SyncColumn column)
        {
            switch (column.GetDbType())
            {
                case DbType.AnsiStringFixedLength:
                    return NpgsqlDbType.Char;
                case DbType.AnsiString:
                    return NpgsqlDbType.Varchar;
                case DbType.Binary:
                    return NpgsqlDbType.Bytea;
                case DbType.Boolean:
                    return NpgsqlDbType.Boolean;
                case DbType.Byte:
                case DbType.SByte:
                    return NpgsqlDbType.Smallint;
                case DbType.Currency:
                    return NpgsqlDbType.Money;
                case DbType.Date:
                    return NpgsqlDbType.Date;
                case DbType.Time:
                    return NpgsqlDbType.Time;
                case DbType.DateTime2:
                    return NpgsqlDbType.Timestamp;
                // https://www.npgsql.org/doc/release-notes/6.0.html DbType.DateTime now maps to timestamptz, not timestamp. DbType.DateTime2 continues to map to timestamp, and DbType.DateTimeOffset continues to map to timestamptz, as before
                case DbType.DateTime:
                case DbType.DateTimeOffset:
                    return NpgsqlDbType.TimestampTz;
                case DbType.Single:
                    return NpgsqlDbType.Real;
                case DbType.Decimal:
                case DbType.VarNumeric:
                    return NpgsqlDbType.Numeric;
                case DbType.Double:
                    return NpgsqlDbType.Double;
                case DbType.Guid:
                    return NpgsqlDbType.Uuid;
                case DbType.Int16:
                case DbType.UInt16:
                    return NpgsqlDbType.Smallint;
                case DbType.Int32:
                case DbType.UInt32:
                    return NpgsqlDbType.Integer;
                case DbType.Int64:
                case DbType.UInt64:
                    return NpgsqlDbType.Bigint;
                case DbType.String:
                case DbType.StringFixedLength:
                case DbType.Xml:
                    return NpgsqlDbType.Text;
                case DbType.Object:
                    return NpgsqlDbType.Array;
            }
            throw new Exception($"this type name {column.GetType().ToString()} is not supported");
        }
        public override byte GetPrecision(SyncColumn columnDefinition)
        {
            var (p, _) = CoercePrecisionAndScale(columnDefinition.Precision, columnDefinition.Scale);

            return p;
        }

        public override (byte precision, byte scale) GetPrecisionAndScale(SyncColumn columnDefinition)
        {
            if (columnDefinition.DbType == (int)DbType.Single && columnDefinition.Precision == 0 && columnDefinition.Scale == 0)
                return (PRECISION_MAX, 8);

            return CoercePrecisionAndScale(columnDefinition.Precision, columnDefinition.Scale);
        }

        public override Type GetType(SyncColumn column)
        {
            switch (GetNpgsqlDbType(column))
            {
                case NpgsqlDbType.Bigint:
                    return Type.GetType("System.Int64");
                case NpgsqlDbType.Double:
                    return Type.GetType("System.Double");
                case NpgsqlDbType.Int2Vector:
                case NpgsqlDbType.Integer:
                    return Type.GetType("System.Int32");
                case NpgsqlDbType.Real:
                    return Type.GetType("System.Single");
                case NpgsqlDbType.Numeric:
                case NpgsqlDbType.Money:
                    return Type.GetType("System.Decimal");
                case NpgsqlDbType.Smallint:
                    return Type.GetType("System.Int16");
                case NpgsqlDbType.Boolean:
                    return Type.GetType("System.Boolean");
                case NpgsqlDbType.Char:
                    return Type.GetType("System.Char");
                case NpgsqlDbType.Text:
                case NpgsqlDbType.Varchar:
                case NpgsqlDbType.Name:
                case NpgsqlDbType.Citext:
                    return Type.GetType("System.String");
                case NpgsqlDbType.Bytea:
                    return Type.GetType("System.Byte[]");
                case NpgsqlDbType.Date:
                case NpgsqlDbType.Timestamp:
                    return Type.GetType("System.DateTime");
                case NpgsqlDbType.Time:
                    return Type.GetType("System.TimeSpan");
                case NpgsqlDbType.TimestampTz:
                case NpgsqlDbType.TimeTz:
                    return typeof(DateTime);
                case NpgsqlDbType.Inet:
                case NpgsqlDbType.Cidr:
                case NpgsqlDbType.MacAddr:
                case NpgsqlDbType.MacAddr8:
                case NpgsqlDbType.Bit:
                case NpgsqlDbType.Varbit:
                case NpgsqlDbType.TsVector:
                case NpgsqlDbType.TsQuery:
                    return Type.GetType("System.String");
                case NpgsqlDbType.Uuid:
                    return Type.GetType("System.Guid");
                case NpgsqlDbType.Xml:
                case NpgsqlDbType.Json:
                case NpgsqlDbType.Jsonb:
                case NpgsqlDbType.Hstore:
                    return Type.GetType("System.String");
                case NpgsqlDbType.Array:
                    return Type.GetType("System.String");
            }
            throw new Exception($"this NpgsqlDbType {GetNpgsqlDbType(column).ToString()} is not supported");

        }
        public override bool IsNumericType(SyncColumn column)
        {
            switch (column.OriginalTypeName.ToLowerInvariant())
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
        public override bool IsReadonly(SyncColumn columnDefinition) => columnDefinition.OriginalTypeName.ToLowerInvariant() == "timestamp" || columnDefinition.IsCompute;
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