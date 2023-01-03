using Dotmim.Sync.Manager;
using System;
using System.Collections.Generic;
using System.Text;
using System.Data;

using Microsoft.Data.Sqlite;

namespace Dotmim.Sync.Sqlite
{
    public class SqliteDbMetadata : DbMetadata
    {

        public override DbType GetDbType(SyncColumn columnDefinition)
        {
            var typeName = columnDefinition.OriginalTypeName.ToLowerInvariant();

            if (typeName.Contains("("))
                typeName = typeName.Substring(0, typeName.IndexOf("("));

            return typeName.ToLowerInvariant() switch
            {
                "bit" => DbType.Boolean,
                "integer" or "bigint" or "smallint" => DbType.Int64,
                "numeric" or "real" or "float" => DbType.Double,
                "decimal" => DbType.Decimal,
                "blob" or "image" => DbType.Binary,
                "datetime" => DbType.DateTime,
                "time" => DbType.Time,
                "text" or "varchar" => DbType.String,
                _ => throw new Exception($"this type {columnDefinition.OriginalTypeName} for column {columnDefinition.ColumnName} is not supported")
            };
        }

        public override object GetOwnerDbType(SyncColumn columnDefinition)
        {
            var typeName = columnDefinition.OriginalTypeName.ToLowerInvariant();

            if (typeName.Contains("("))
                typeName = typeName.Substring(0, typeName.IndexOf("("));

            return typeName.ToLowerInvariant() switch
            {
                "bit" or "integer" or "bigint" or "smallint" => SqliteType.Integer,
                "numeric" or "decimal" or "real" or "float" => SqliteType.Real,
                "blob" or "image" => SqliteType.Blob,
                "datetime" or "time" or "varchar" or "text" => SqliteType.Text,
                _ => throw new Exception($"Type '{columnDefinition.OriginalTypeName.ToLowerInvariant()}' (column {columnDefinition.ColumnName}) is not supported"),
            };
        }

        public SqliteType GetOwnerDbTypeFromDbType(SyncColumn columnDefinition) => columnDefinition.GetDbType() switch
        {
            DbType.AnsiString or DbType.AnsiStringFixedLength or DbType.String or DbType.StringFixedLength or DbType.Xml or 
            DbType.Time or DbType.DateTimeOffset or DbType.Guid or DbType.Date or DbType.DateTime or DbType.DateTime2 => SqliteType.Text,
            DbType.Binary or DbType.Object => SqliteType.Blob,
            DbType.Boolean or DbType.Byte or DbType.Int16 or DbType.Int32 or DbType.UInt16 or DbType.Int64 or DbType.UInt32 or 
            DbType.UInt64 or DbType.SByte => SqliteType.Integer,
            DbType.Decimal or DbType.Double or DbType.Single or DbType.Currency or DbType.VarNumeric => SqliteType.Real,
            _ => throw new Exception($"In Column {columnDefinition.ColumnName}, the type {columnDefinition.GetDbType()} is not supported"),
        };

        public SqliteType GetSqliteType(SyncColumn column) => (SqliteType)this.GetOwnerDbType(column);

        public override Type GetType(SyncColumn columnDefinition)
        {
            var dbType = (SqliteType)GetOwnerDbType(columnDefinition);

            return dbType switch
            {
                SqliteType.Integer => typeof(long),
                SqliteType.Real => typeof(double),
                SqliteType.Text => typeof(string),
                SqliteType.Blob => typeof(object),
                _ => throw new Exception($"In Column {columnDefinition.ColumnName}, the type {dbType} is not supported"),
            };
        }

        public override int GetMaxLength(SyncColumn columnDefinition) 
            => columnDefinition.MaxLength > int.MaxValue ? int.MaxValue : Convert.ToInt32(columnDefinition.MaxLength);

        public override (byte precision, byte scale) GetPrecisionAndScale(SyncColumn columnDefinition) => (0, 0);

        public override byte GetPrecision(SyncColumn columnDefinition) => columnDefinition.Precision;

        public override bool IsSupportingScale(SyncColumn columnDefinition) => GetSqliteType(columnDefinition) == SqliteType.Real;

        public override bool IsNumericType(SyncColumn columnDefinition)
        {
            var sqliteType = GetSqliteType(columnDefinition);
            return sqliteType == SqliteType.Integer || sqliteType == SqliteType.Real;
        }

        public bool IsTextType(SyncColumn columnDefinition)
        {
            var dbType = (DbType)columnDefinition.DbType;

            return dbType switch
            {
                DbType.AnsiString or DbType.AnsiStringFixedLength or 
                DbType.String or DbType.StringFixedLength or 
                DbType.Xml or DbType.Guid => true,
                _ => false,
            };
        }

        public override bool IsValid(SyncColumn columnDefinition)
        {
            var typeName = columnDefinition.OriginalTypeName.ToLowerInvariant();

            if (typeName.Contains("("))
                typeName = typeName.Substring(0, typeName.IndexOf("("));

            return typeName switch
            {
                "integer" or "float" or "decimal" or "bit" or "bigint" or "numeric" or "blob" or "image" or 
                "datetime" or "time" or "text" or "varchar" or "real" or "smallint" => true,
                _ => false,
            };
        }

        public override bool IsReadonly(SyncColumn columnDefinition) => false;


        // ----------------------------------------------------------------------------------


        /// <summary>
        /// Gets a compatible column definition
        /// </summary>
        public string GetCompatibleColumnTypeDeclarationString(SyncColumn column, string fromProviderType)
        {
            if (fromProviderType == SqliteSyncProvider.ProviderType)
                return column.OriginalTypeName;

            // Fallback on my sql db type extract from simple db type
            var sqliteType = this.GetOwnerDbTypeFromDbType(column);

            return sqliteType switch
            {
                SqliteType.Integer => "integer",
                SqliteType.Real => "numeric",
                SqliteType.Text => "text",
                SqliteType.Blob => "blob",
                _ => throw new Exception($"In Column {column.ColumnName}, the type {column.GetDbType()} is not supported"),
            };

        }

    }
}
