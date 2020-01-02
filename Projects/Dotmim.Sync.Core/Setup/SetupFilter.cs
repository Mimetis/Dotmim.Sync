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
    public class SetupFilter : SyncColumnIdentifier, IEquatable<SetupFilter>
    {
        /// <summary>
        /// Gets or Sets the column type. If specified, we have a virtual filter
        /// </summary>
        public int? ColumnType { get; set; }

        /// <summary>
        /// Gets whether the filter is targeting an existing column of the target table (not virtual) or it is only used as a parameter in the selectchanges stored procedure (virtual)
        /// </summary>
        public bool IsVirtual => ColumnType.HasValue;

        /// <summary>
        /// Creates a filterclause allowing to specify a different DbType.
        /// If you specify the columnType, Dotmim.Sync will expect that the column does not exist on the table, and the filter is only
        /// used as a parameter for the selectchanges stored procedure. Thus, IsVirtual would be true
        /// </summary>
        public SetupFilter(string tableName, string columnName, string schemaName = null,  int? columnType = null)
        {
            this.ColumnName = columnName;
            this.TableName = tableName;
            this.SchemaName = schemaName;
            this.ColumnType = columnType;
        }

        /// <summary>
        /// For Serializer
        /// </summary>
        public SetupFilter()
        {
        }

        public override bool Equals(object obj)
        {
            return this.Equals(obj as SetupFilter);
        }

        public bool Equals(SetupFilter other)
        {
            if (other == null)
                return false;

            var sc = SyncGlobalization.DataSourceStringComparison;

            var sn = this.SchemaName == null ? string.Empty : this.SchemaName;
            var otherSn = other.SchemaName == null ? string.Empty : other.SchemaName;

            return other != null &&
                   this.TableName.Equals(other.TableName, sc) &&
                   this.ColumnName.Equals(other.ColumnName, sc) && 
                   sn.Equals(otherSn, sc);
        }

        public override int GetHashCode()
        {
            var hashCode = -1896683325;
            hashCode = hashCode * -1521134295 + this.ColumnName.GetHashCode();
            hashCode = hashCode * -1521134295 + this.TableName.GetHashCode();
            hashCode = hashCode * -1521134295 + this.SchemaName.GetHashCode();
            hashCode = hashCode * -1521134295 + this.IsVirtual.GetHashCode();
            return hashCode;
        }

        public static bool operator ==(SetupFilter left, SetupFilter right)
        {
            return EqualityComparer<SetupFilter>.Default.Equals(left, right);
        }

        public static bool operator !=(SetupFilter left, SetupFilter right)
        {
            return !(left == right);
        }
    }
}
