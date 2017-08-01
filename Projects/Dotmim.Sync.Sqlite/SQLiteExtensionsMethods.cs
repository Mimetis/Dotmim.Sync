using Dotmim.Sync.Core;
using Dotmim.Sync.Core.Common;
using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Linq;
using System.Data.SQLite;

namespace Dotmim.Sync.SQLite
{
    public static class SQLiteExtensionsMethods
    {

        private static TypeAffinity[] _typecodeAffinities = {
          TypeAffinity.Null,     // Empty (0)
          TypeAffinity.Blob,     // Object (1)
          TypeAffinity.Null,     // DBNull (2)
          TypeAffinity.Int64,    // Boolean (3)
          TypeAffinity.Int64,    // Char (4)
          TypeAffinity.Int64,    // SByte (5)
          TypeAffinity.Int64,    // Byte (6)
          TypeAffinity.Int64,    // Int16 (7)
          TypeAffinity.Int64,    // UInt16 (8)
          TypeAffinity.Int64,    // Int32 (9)
          TypeAffinity.Int64,    // UInt32 (10)
          TypeAffinity.Int64,    // Int64 (11)
          TypeAffinity.Int64,    // UInt64 (12)
          TypeAffinity.Double,   // Single (13)
          TypeAffinity.Double,   // Double (14)
          TypeAffinity.Double,   // Decimal (15)
          TypeAffinity.DateTime, // DateTime (16)
          TypeAffinity.Null,     // ?? (17)
          TypeAffinity.Text      // String (18)
        };
        private static DbType[] _typetodbtype = {
              DbType.Object,   // Empty (0)
              DbType.Binary,   // Object (1)
              DbType.Object,   // DBNull (2)
              DbType.Boolean,  // Boolean (3)
              DbType.SByte,    // Char (4)
              DbType.SByte,    // SByte (5)
              DbType.Byte,     // Byte (6)
              DbType.Int16,    // Int16 (7)
              DbType.UInt16,   // UInt16 (8)
              DbType.Int32,    // Int32 (9)
              DbType.UInt32,   // UInt32 (10)
              DbType.Int64,    // Int64 (11)
              DbType.UInt64,   // UInt64 (12)
              DbType.Single,   // Single (13)
              DbType.Double,   // Double (14)
              DbType.Decimal,  // Decimal (15)
              DbType.DateTime, // DateTime (16)
              DbType.Object,   // ?? (17)
              DbType.String    // String (18)
            };
        private static object[] _dbtypetonumericprecision = {
          DBNull.Value, // AnsiString (0)
          DBNull.Value, // Binary (1)
          3,            // Byte (2)
          DBNull.Value, // Boolean (3)
          19,           // Currency (4)
          DBNull.Value, // Date (5)
          DBNull.Value, // DateTime (6)
          53,           // Decimal (7)
          53,           // Double (8)
          DBNull.Value, // Guid (9)
          5,            // Int16 (10)
          10,           // Int32 (11)
          19,           // Int64 (12)
          DBNull.Value, // Object (13)
          3,            // SByte (14)
          24,           // Single (15)
          DBNull.Value, // String (16)
          DBNull.Value, // Time (17)
          5,            // UInt16 (18)
          10,           // UInt32 (19)
          19,           // UInt64 (20)
          53,           // VarNumeric (21)
          DBNull.Value, // AnsiStringFixedLength (22)
          DBNull.Value, // StringFixedLength (23)
          DBNull.Value, // ?? (24)
          DBNull.Value  // Xml (25)
        };
        private static int[] _dbtypetocolumnsize = {
              int.MaxValue, // AnsiString (0)
              int.MaxValue, // Binary (1)
              1,            // Byte (2)
              1,            // Boolean (3)
              8,            // Currency (4)
              8,            // Date (5)
              8,            // DateTime (6)
              8,            // Decimal (7)
              8,            // Double (8)
              32,           // Guid (9)
              2,            // Int16 (10)
              4,            // Int32 (11)
              8,            // Int64 (12)
              int.MaxValue, // Object (13)
              1,            // SByte (14)
              4,            // Single (15)
              int.MaxValue, // String (16)
              8,            // Time (17)
              2,            // UInt16 (18)
              4,            // UInt32 (19)
              8,            // UInt64 (20)
              8,            // VarNumeric (21)
              int.MaxValue, // AnsiStringFixedLength (22)
              int.MaxValue, // StringFixedLength (23)
              int.MaxValue, // ?? (24)
              int.MaxValue  // Xml (25)
        };
        private static Type[] _dbtypeToType = {
          typeof(string),   // AnsiString (0)
          typeof(byte[]),   // Binary (1)
          typeof(byte),     // Byte (2)
          typeof(bool),     // Boolean (3)
          typeof(decimal),  // Currency (4)
          typeof(DateTime), // Date (5)
          typeof(DateTime), // DateTime (6)
          typeof(decimal),  // Decimal (7)
          typeof(double),   // Double (8)
          typeof(Guid),     // Guid (9)
          typeof(Int16),    // Int16 (10)
          typeof(Int32),    // Int32 (11)
          typeof(Int64),    // Int64 (12)
          typeof(object),   // Object (13)
          typeof(sbyte),    // SByte (14)
          typeof(float),    // Single (15)
          typeof(string),   // String (16)
          typeof(DateTime), // Time (17)
          typeof(UInt16),   // UInt16 (18)
          typeof(UInt32),   // UInt32 (19)
          typeof(UInt64),   // UInt64 (20)
          typeof(double),   // VarNumeric (21)
          typeof(string),   // AnsiStringFixedLength (22)
          typeof(string),   // StringFixedLength (23)
          typeof(string),   // ?? (24)
          typeof(string),   // Xml (25)
        };

        /// <summary>
        /// For a given intrinsic type, return a DbType compatible with SQLite
        /// </summary>
        internal static DbType ToSQLiteDbType(this Type typ)
        {
            TypeCode tc = Type.GetTypeCode(typ);
            if (tc == TypeCode.Object)
            {
                if (typ == typeof(byte[])) return DbType.Binary;
                if (typ == typeof(Guid)) return DbType.Guid;
                return DbType.String;
            }
            return _typetodbtype[(int)tc];
        }
    
        /// <summary>
        /// Convert a DbType to a Type
        /// </summary>
        /// <param name="typ">The DbType to convert from</param>
        /// <returns>The closest-match .NET type</returns>
        internal static Type DbTypeToType(DbType typ)
        {
            return _dbtypeToType[(int)typ];
        }

        /// <summary>
        /// For a given type, return the closest-match SQLite TypeAffinity, which only understands a very limited subset of types.
        /// </summary>
        /// <param name="typ">The type to evaluate</param>
        /// <returns>The SQLite type affinity for that type.</returns>
        internal static TypeAffinity ToSQLiteAffinity(this Type typ)
        {
            TypeCode tc = Type.GetTypeCode(typ);
            if (tc == TypeCode.Object)
            {
                if (typ == typeof(byte[]))
                    return TypeAffinity.Blob;
                else
                    return TypeAffinity.Text;
            }
            return _typecodeAffinities[(int)tc];
        }

        internal static string GetSQLiteTypePrecisionString(this DmColumn column)
        {
            var typeAffinity = column.DataType.ToSQLiteAffinity();

            if (typeAffinity == TypeAffinity.Text)
                return column.MaxLength > 0 ? $"({column.MaxLength})" : "";

            return "";
        }

        internal static string GetSqlDbTypeString(this DmColumn column)
        {
            var typeAffinity = column.DataType.ToSQLiteAffinity();

            switch (typeAffinity)
            {
                case TypeAffinity.Int64:
                    return "integer";
                case TypeAffinity.Double:
                    return "numeric";
                case TypeAffinity.Blob:
                    return "blob";
                case TypeAffinity.DateTime:
                    return "datetime";
                default:
                    return "text";
            }
        }

    
        internal static SQLiteParameter GetSQLiteParameter(this DmColumn column)
        {
            SQLiteParameter sqliteParameter = new SQLiteParameter();
            sqliteParameter.ParameterName = $"@{column.ColumnName}";
            sqliteParameter.IsNullable = column.AllowDBNull;
            return sqliteParameter;
        }

     

    }
}
