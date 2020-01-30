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
    public class SyncFilter : IDisposable, IEquatable<SyncFilter>
    {

        [DataMember(Name = "t", IsRequired = true, Order = 1)]
        public string TableName { get; set; }

        [DataMember(Name = "s", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public string SchemaName { get; set; }


        /// <summary>
        /// Gets or Sets the parameters list, used as input in the stored procedure
        /// </summary>
        [DataMember(Name = "p", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public SyncFilterParameters Parameters { get; set;  } = new SyncFilterParameters();

        /// <summary>
        /// Gets or Sets side where filters list
        /// </summary>
        [DataMember(Name = "w", IsRequired = false, EmitDefaultValue = false, Order = 4)]
        public SyncFilterWhereSideItems Wheres { get; set; } = new SyncFilterWhereSideItems();

        /// <summary>
        /// Gets or Sets side where filters list
        /// </summary>
        [DataMember(Name = "j", IsRequired = false, EmitDefaultValue = false, Order = 5)]
        public SyncFilterJoins Joins { get; set; } = new SyncFilterJoins();

        /// <summary>
        /// Gets or Sets customs where
        /// </summary>
        [DataMember(Name = "cw", IsRequired = false, EmitDefaultValue = false, Order = 6)]
        public List<string> CustomWheres { get; set; } = new List<string>();


        /// <summary>
        /// Gets the ShemaFilter's SyncSchema
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; set; }


        /// <summary>
        /// Creates a filterclause allowing to specify a different DbType.
        /// If you specify the columnType, Dotmim.Sync will expect that the column does not exist on the table, and the filter is only
        /// used as a parameter for the selectchanges stored procedure. Thus, IsVirtual would be true
        /// </summary>
        public SyncFilter(string tableName, string schemaName = null)
        {
            this.TableName = tableName;
            this.SchemaName = schemaName;
        }


        /// <summary>
        /// Clone the SyncFilter
        /// </summary>
        public SyncFilter Clone()
        {
            var clone = new SyncFilter();
            clone.SchemaName = this.SchemaName;
            clone.TableName = this.TableName;

            return clone;
        }


        /// <summary>
        /// Ensure filter has the correct schema (since the property is not serialized
        /// </summary>
        public void EnsureFilter(SyncSet schema)
        {
            this.Schema = schema;

            this.Parameters.EnsureFilters(this.Schema);
            this.Wheres.EnsureFilters(this.Schema);
            this.Joins.EnsureFilters(this.Schema);
        }

        /// <summary>
        /// For Serializer
        /// </summary>
        public SyncFilter()
        {
        }


        /// <summary>
        /// Clear
        /// </summary>
        public void Clear() => this.Dispose(true);


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

        public override bool Equals(object obj) 
            => this.Equals(obj as SyncFilter);

        public bool Equals(SyncFilter other)
        {
            if (other == null)
                return false;

            var sc = SyncGlobalization.DataSourceStringComparison;

            var sn = this.SchemaName == null ? string.Empty : this.SchemaName;
            var otherSn = other.SchemaName == null ? string.Empty : other.SchemaName;

            return other != null &&
                   this.TableName.Equals(other.TableName, sc) &&
                   sn.Equals(otherSn, sc);
        }

        public override int GetHashCode() 
            => 1951375558 + EqualityComparer<SyncSet>.Default.GetHashCode(this.Schema);

        public static bool operator ==(SyncFilter left, SyncFilter right) 
            => EqualityComparer<SyncFilter>.Default.Equals(left, right);

        public static bool operator !=(SyncFilter left, SyncFilter right) 
            => !(left == right);
    }
}
