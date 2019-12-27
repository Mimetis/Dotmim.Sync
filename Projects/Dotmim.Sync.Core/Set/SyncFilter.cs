using System;
using System.Collections.Generic;
using System.Data;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    /// <summary>
    /// Design a filter clause on Dmtable
    /// </summary>
    [DataContract(Name = "sf"), Serializable]
    public class SyncFilter : SyncColumnIdentifier, IDisposable
    {
        [DataMember(Name = "dt", IsRequired = false, EmitDefaultValue = false, Order = 4)]
        public int? ColumnType { get; set; }


        /// <summary>
        /// Gets the ShemaFilter's SyncSchema
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; set; }

        /// <summary>
        /// Gets whether the filter is targeting an existing column of the target table (not virtual) or it is only used as a parameter in the selectchanges stored procedure (virtual)
        /// </summary>
        [IgnoreDataMember]
        public bool IsVirtual => ColumnType.HasValue;

        /// <summary>
        /// Creates a filterclause allowing to specify a different DbType.
        /// If you specify the columnType, Dotmim.Sync will expect that the column does not exist on the table, and the filter is only
        /// used as a parameter for the selectchanges stored procedure. Thus, IsVirtual would be true
        /// </summary>
        public SyncFilter(SyncColumn schemaColumn, int? columnType = null)
        {
            this.ColumnName = schemaColumn.ColumnName;
            this.TableName = schemaColumn.Table.TableName;
            this.SchemaName = schemaColumn.Table.SchemaName;
            this.ColumnType = columnType;
        }


        /// <summary>
        /// Creates a filterclause allowing to specify a different DbType.
        /// If you specify the columnType, Dotmim.Sync will expect that the column does not exist on the table, and the filter is only
        /// used as a parameter for the selectchanges stored procedure. Thus, IsVirtual would be true
        /// </summary>
        public SyncFilter(string tableName, string columnName, string schemaName = null,  int? columnType = null)
        {
            this.ColumnName = columnName;
            this.TableName = tableName;
            this.SchemaName = schemaName ?? string.Empty;
            this.ColumnType = columnType;
        }

        /// <summary>
        /// Clone the SyncFilter
        /// </summary>
        public new SyncFilter Clone()
        {
            var clone = new SyncFilter();
            clone.ColumnName = this.ColumnName;
            clone.SchemaName = this.SchemaName;
            clone.TableName = this.TableName;
            clone.ColumnType = this.ColumnType;

            return clone;
        }


        /// <summary>
        /// Ensure filter has the correct schema (since the property is not serialized
        /// </summary>
        public void EnsureFilter(SyncSet schema) => this.Schema = schema;

        /// <summary>
        /// For Serializer
        /// </summary>
        public SyncFilter()
        {
        }

        /// <summary>
        /// Get DbType if specified
        /// </summary>
        public DbType? GetDbType()
        {
            if (!this.ColumnType.HasValue)
                return null;

            return (DbType)this.ColumnType.Value;
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool cleanup)
        {
            // Dispose managed ressources
            if (cleanup)
            {
                // clean rows
                this.Schema = null;
            }

            // Dispose unmanaged ressources
        }
    }
}
