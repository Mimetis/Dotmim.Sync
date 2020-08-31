using Dotmim.Sync.Manager;
using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Linq;
using NpgsqlTypes;

namespace Dotmim.Sync.Postgres
{
    public class NpgsqlDbMetadata : DbMetadata
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
                    return DbType.AnsiStringFixedLength;


                case "character varying":
                case "varchar":
                case "refcursor":
                case "citext":
                case "text":
                    return DbType.AnsiString;

                case "date":
                    return DbType.Date;

                case "timestamp":
                    return DbType.DateTime;

                case "timestamptz":
                case "timetz":
                    return DbType.DateTimeOffset;

                case "time":
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

            }
            throw new Exception($"this type {typeName} is not supported");
        }

        /// <summary>
        /// Gets the NpgsqlDbType issued from the server type name
        /// </summary>
        public override object ValidateOwnerDbType(string typeName, bool isUnsigned, bool isUnicode, long maxLength)
        {

            switch (typeName.ToLowerInvariant())
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
                    return NpgsqlDbType.Time;
                case "timestamp":
                    return NpgsqlDbType.Timestamp;
                case "timestamptz":
                    return NpgsqlDbType.TimestampTz;
                case "timetz":
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
            throw new Exception($"this type name {typeName} is not supported");
        }

        /// <summary>
        /// Gets the max length autorized
        /// </summary>
        public override int ValidateMaxLength(string typeName, bool isUnsigned, bool isUnicode, long maxLength)
        {
            var iMaxLength = maxLength > 8000 ? 8000 : Convert.ToInt32(maxLength);

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
                    return "bytea";
                case DbType.Boolean:
                    return "boolean";
                case DbType.Byte:
                    return "smallint";
                case DbType.Currency:
                    return "money";
                case DbType.Date:
                    return "date";
                case DbType.DateTime:
                    return "timestamp";
                case DbType.DateTime2:
                    return "timestamp";
                case DbType.DateTimeOffset:
                    return "timestamptz";
                case DbType.Double:
                    return "double precision";
                case DbType.Single:
                    return "real";
                case DbType.Decimal:
                case DbType.VarNumeric:
                    return "numeric";
                case DbType.Guid:
                    return "uuid";
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
                    return "varchar";
                case DbType.Time:
                    return "time";
                case DbType.Object:
                    return "bytea";
            }
            throw new Exception($"this DbType {dbType.ToString()} is not supported");
        }

        /// <summary>
        /// Gets a npgsql type name form a NpgsqlType enum value
        /// </summary>
        public override string GetStringFromOwnerDbType(object ownerType)
        {
            NpgsqlDbType npgsqlDbType = (NpgsqlDbType)ownerType;


            switch (npgsqlDbType)
            {
                case NpgsqlDbType.Bigint:
                    return "bigint";
                case NpgsqlDbType.Double:
                    return "double precision";
                case NpgsqlDbType.Integer:
                    return "integer";
                case NpgsqlDbType.Numeric:
                    return "numeric";
                case NpgsqlDbType.Real:
                    return "real";
                case NpgsqlDbType.Smallint:
                    return "smallint";
                case NpgsqlDbType.Money:
                    return "money";
                case NpgsqlDbType.Boolean:
                    return "boolean";
                case NpgsqlDbType.Char:
                    return "char";
                case NpgsqlDbType.Text:
                    return "text";
                case NpgsqlDbType.Varchar:
                    return "varchar";
                case NpgsqlDbType.Name:
                    return "name";
                case NpgsqlDbType.Citext:
                    return "citext";
                case NpgsqlDbType.Bytea:
                    return "bytea";
                case NpgsqlDbType.Date:
                    return "date";
                case NpgsqlDbType.Time:
                    return "time";
                case NpgsqlDbType.Timestamp:
                    return "timestamp";
                case NpgsqlDbType.TimestampTz:
                    return "timestamptz";
                case NpgsqlDbType.TimeTz:
                    return "timetz";
                case NpgsqlDbType.Inet:
                    return "inet";
                case NpgsqlDbType.Cidr:
                    return "cidr";
                case NpgsqlDbType.MacAddr:
                    return "macaddr";
                case NpgsqlDbType.MacAddr8:
                    return "macaddr8";
                case NpgsqlDbType.Bit:
                    return "bit";
                case NpgsqlDbType.Varbit:
                    return "varbit";
                case NpgsqlDbType.TsVector:
                    return "tsvector";
                case NpgsqlDbType.TsQuery:
                    return "tsquery";
                case NpgsqlDbType.Uuid:
                    return "uuid";
                case NpgsqlDbType.Xml:
                    return "xml";
                case NpgsqlDbType.Json:
                    return "json";
                case NpgsqlDbType.Jsonb:
                    return "jsonb";
                case NpgsqlDbType.Hstore:
                    return "hstore";
                case NpgsqlDbType.Array:
                    return "array";
                case NpgsqlDbType.Refcursor:
                    return "refcursor";
                case NpgsqlDbType.Int2Vector:
                    return "int2vector";
                case NpgsqlDbType.Oid:
                    return "";
                case NpgsqlDbType.Xid:
                    return "xid";
                case NpgsqlDbType.Cid:
                    return "cidr";
            }


            throw new Exception($"this NpgsqlDbType {ownerType.ToString()} is not supported");
        }


        public override string GetPrecisionStringFromDbType(DbType dbType, int maxLength, byte precision, byte scale)
        {
            switch (dbType)
            {
                case DbType.AnsiString:
                    if (maxLength > 0 && maxLength <= 8000)
                        return $"({maxLength})";
                    else
                        return $"(8000)";
                case DbType.String:
                    if (maxLength > 0 && maxLength <= 4000)
                        return $"({maxLength})";
                    else
                        return $"(4000)";
                case DbType.AnsiStringFixedLength:
                case DbType.Binary:
                    if (maxLength > 0 && maxLength <= 8000)
                        return $"({maxLength})";
                    else
                        return $"";
                case DbType.StringFixedLength:
                    return $"({Math.Min(4000, maxLength)})";
                case DbType.Decimal:
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
            NpgsqlDbType sqlDbType = (NpgsqlDbType)ownerDbType;
            switch (sqlDbType)
            {
                case NpgsqlDbType.Varchar:
                    if (maxLength > 0 && maxLength <= 8000)
                        return $"({maxLength})";
                    else
                        return "";
                case NpgsqlDbType.Numeric:
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
        /// Gets the corresponding NpgsqlDbType from a classic DbType
        /// </summary>
        public override object GetOwnerDbTypeFromDbType(DbType dbType)
        {
            // Fallback on DbType
            switch (dbType)
            {
                case DbType.AnsiString:
                case DbType.AnsiStringFixedLength:
                    return NpgsqlDbType.Varchar;
                case DbType.Binary:
                    return NpgsqlDbType.Bytea;
                case DbType.Boolean:
                    return NpgsqlDbType.Boolean;
                case DbType.Byte:
                    return NpgsqlDbType.Smallint;
                case DbType.Currency:
                    return NpgsqlDbType.Money;
                case DbType.Date:
                    return NpgsqlDbType.Date;
                case DbType.DateTime:
                    return NpgsqlDbType.Timestamp;
                case DbType.DateTime2:
                    return NpgsqlDbType.Timestamp;
                case DbType.DateTimeOffset:
                    return NpgsqlDbType.TimestampTz;
                case DbType.Single:
                    return NpgsqlDbType.Real;
                case DbType.Decimal:
                case DbType.Double:
                case DbType.VarNumeric:
                    return NpgsqlDbType.Double;
                case DbType.Guid:
                    return NpgsqlDbType.Uuid;
                case DbType.Int16:
                    return NpgsqlDbType.Smallint;
                case DbType.Int32:
                case DbType.UInt16:
                    return NpgsqlDbType.Integer;
                case DbType.Int64:
                case DbType.UInt32:
                case DbType.UInt64:
                    return NpgsqlDbType.Bigint;
                case DbType.SByte:
                    return NpgsqlDbType.Smallint;
                case DbType.String:
                case DbType.StringFixedLength:
                case DbType.Xml:
                    return NpgsqlDbType.Varchar;
                case DbType.Time:
                    return NpgsqlDbType.Time;
                case DbType.Object:
                    return NpgsqlDbType.Bytea;
            }

            throw new Exception($"this type {dbType} is not supported");

        }

        /// <summary>
        /// Gets a managed type from a NpgsqlDbType
        /// </summary>
        public override Type ValidateType(object ownerType)
        {
            NpgsqlDbType sqlDbType = (NpgsqlDbType)ownerType;

            switch (sqlDbType)
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
                    return Type.GetType("System.DateTimeOffset");
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
            }
            throw new Exception($"this NpgsqlDbType {ownerType.ToString()} is not supported");
        }

        public override bool SupportScale(string typeName)
        {
            switch (typeName.ToLowerInvariant())
            {
                case "decimal":
                case "numeric":
                    return true;
            }
            return false;
        }
        public override bool IsNumericType(string typeName)
        {
            switch (typeName.ToLowerInvariant())
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

        public override bool IsTextType(string typeName)
        {
            switch (typeName.ToLowerInvariant())
            {
                case "xml":
                case "json":
                case "jsonb":
                case "hstore":
                case "character varying":
                case "varchar":
                case "refcursor":
                case "citext":
                case "text":
                case "character":
                case "char":
                case "name":
                case "bit":
                case "varbit":
                case "bit varying":
                case "cid":
                case "cidr":
                case "inet":
                case "macaddr":
                case "macaddr8":
                case "tsquery":
                case "tsvector":
                    return true;
            }
            return false;
        }

        public override bool IsValid(SyncColumn columnDefinition)
        {
            switch (columnDefinition.OriginalTypeName.ToLowerInvariant())
            {
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
                case "timestamptz":
                case "timetz":
                case "time":
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
                    return true;
            }
            return false;
        }



        public override bool ValidateIsReadonly(SyncColumn columnDefinition)
        {
            return columnDefinition.IsCompute;
        }

        public override byte ValidatePrecision(SyncColumn columnDefinition)
        {
            var (p, s) = CoercePrecisionAndScale(columnDefinition.Precision, columnDefinition.Scale);

            return p;
        }

        public override (byte precision, byte scale) GetPrecisionFromOwnerDbType(object ownerDbType, byte precision, byte scale)
        {
            NpgsqlDbType sqlDbType = (NpgsqlDbType)ownerDbType;
            switch (sqlDbType)
            {
                case NpgsqlDbType.Numeric:
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
            return maxLength;
        }

        public override (byte precision, byte scale) ValidatePrecisionAndScale(SyncColumn columnDefinition)
        {
            return CoercePrecisionAndScale(columnDefinition.Precision, columnDefinition.Scale);
        }
    }

}

