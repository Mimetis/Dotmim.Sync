using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    [DataContract(Name = "sfwsi"), Serializable]
    public class SyncFilterWhereSideItem : SyncColumnIdentifier, IEquatable<SyncFilterWhereSideItem>
    {

        [DataMember(Name = "p", IsRequired = true, Order = 4)]
        public String ParameterName { get; set; }


        /// <summary>
        /// Gets the ShemaTable's SyncSchema
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; set; }


        /// <summary>
        /// Ensure filter parameter as the correct schema (since the property is not serialized)
        /// </summary>
        public void EnsureFilterWhereSideItem(SyncSet schema)
        {
            this.Schema = schema;
        }


        public bool Equals(SyncFilterWhereSideItem other)
        {
            if (other == null)
                return false;

            var sc = SyncGlobalization.DataSourceStringComparison;

            var sn = this.SchemaName == null ? string.Empty : this.SchemaName;
            var otherSn = other.SchemaName == null ? string.Empty : other.SchemaName;

            return this.ParameterName.Equals(other.ParameterName, sc) &&
                   this.ColumnName.Equals(other.ColumnName, sc) &&
                   this.TableName.Equals(other.TableName, sc) &&
                   sn.Equals(otherSn, sc);
        }

        public override bool Equals(object obj) => this.Equals(obj as SyncFilterWhereSideItem);

        public override int GetHashCode() => base.GetHashCode();

        public static bool operator ==(SyncFilterWhereSideItem left, SyncFilterWhereSideItem right)
            => EqualityComparer<SyncFilterWhereSideItem>.Default.Equals(left, right);

        public static bool operator !=(SyncFilterWhereSideItem left, SyncFilterWhereSideItem right)
            => !(left == right);
    }
}
