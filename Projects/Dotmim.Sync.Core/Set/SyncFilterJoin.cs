using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    [DataContract(Name = "sfj"), Serializable]
    public class SyncFilterJoin : IEquatable<SyncFilterJoin>
    {

        /// <summary>
        /// Ensure filter parameter as the correct schema (since the property is not serialized)
        /// </summary>
        public void EnsureFilterJoin(SyncSet schema)
        {
            this.Schema = schema;
        }

        /// <summary>
        /// Gets the ShemaTable's SyncSchema
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; set; }

        [DataMember(Name = "je", IsRequired = true, Order = 1)]
        public Join JoinEnum { get; set; }

        [DataMember(Name = "tbl", IsRequired = true, Order = 2)]
        public string TableName { get; set; }

        [DataMember(Name = "ltbl", IsRequired = true, Order = 3)]
        public string LeftTableName { get; set; }

        [DataMember(Name = "lcol", IsRequired = true, Order = 4)]
        public string LeftColumnName { get; set; }

        [DataMember(Name = "rtbl", IsRequired = true, Order = 5)]
        public string RightTableName { get; set; }

        [DataMember(Name = "rcol", IsRequired = true, Order = 6)]
        public string RightColumnName { get; set; }

        public SyncFilterJoin()
        {

        }

        public bool Equals(SyncFilterJoin other)
        {
            if (other == null)
                return false;

            var sc = SyncGlobalization.DataSourceStringComparison;

            return this.JoinEnum == other.JoinEnum
                && this.TableName.Equals(other.TableName, sc)
                && this.LeftColumnName.Equals(other.LeftColumnName, sc)
                && this.LeftTableName.Equals(other.LeftTableName, sc)
                && this.RightColumnName.Equals(other.RightColumnName, sc)
                && this.RightTableName.Equals(other.RightTableName, sc);
        }

        public override bool Equals(object obj) => this.Equals(obj as SyncFilterJoin);

        public override int GetHashCode() => base.GetHashCode();

        public static bool operator ==(SyncFilterJoin left, SyncFilterJoin right)
            => EqualityComparer<SyncFilterJoin>.Default.Equals(left, right);

        public static bool operator !=(SyncFilterJoin left, SyncFilterJoin right)
            => !(left == right);
    }

}
