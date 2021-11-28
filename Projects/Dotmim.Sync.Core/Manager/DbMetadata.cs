
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace Dotmim.Sync.Manager
{
    public abstract class DbMetadata
    {

        /// <summary>
        /// Validate if a column definition is actualy supported by the provider
        /// </summary>
        public abstract bool IsValid(SyncColumn columnDefinition);

        /// <summary>
        /// Gets and validate a max length issued from the database definition
        /// </summary>
        public abstract int GetMaxLength(SyncColumn columnDefinition);

        /// <summary>
        /// Get the native datastore DbType (that's why we return object instead of SqlDbType or SqliteDbType or MySqlDbType)
        /// </summary>
        public abstract object GetOwnerDbType(SyncColumn columnDefinition);

        /// <summary>
        /// Get a DbType from a datastore type name
        /// </summary>
        public abstract DbType GetDbType(SyncColumn columnDefinition);

        /// <summary>
        /// Validate if a column is readonly or not
        /// </summary>
        /// <param name="columnDefinition"></param>
        /// <returns></returns>
        public abstract bool IsReadonly(SyncColumn columnDefinition);

        /// <summary>
        /// Check if a type name is a numeric type
        /// </summary>
        public abstract bool IsNumericType(SyncColumn columnDefinition);

        /// <summary>
        /// Check if a type name support scale
        /// </summary>
        public abstract bool IsSupportingScale(SyncColumn columnDefinition);

        /// <summary>
        /// Get precision and scale from a SchemaColumn
        /// </summary>
        public abstract (byte precision, byte scale) GetPrecisionAndScale(SyncColumn columnDefinition);

        /// <summary>
        /// Get precision if supported (MySql supports int(10))
        /// </summary>
        public abstract byte GetPrecision(SyncColumn columnDefinition);

        /// <summary>
        /// Get a managed type from a datastore dbType
        /// </summary>
        public abstract Type GetType(SyncColumn columnDefinition);

    }
}
