using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync.Setup
{
    [DataContract(Name = "sfw"), Serializable]
    public class SetupFilterWhere :IEquatable<SetupFilterWhere>
    {
        [DataMember(Name = "tn", IsRequired = true, Order = 1)]
        public string TableName { get; set; }

        [DataMember(Name = "sn", IsRequired = false, EmitDefaultValue =false, Order = 2)]
        public string SchemaName { get; set; }

        [DataMember(Name = "cn", IsRequired = true, Order = 3)]
        public string ColumnName { get; set; }

        [DataMember(Name = "pn", IsRequired = true, Order = 4)]
        public string ParameterName { get; set; }

        public bool Equals(SetupFilterWhere other)
        {
            if (other == null)
                return false;

            var sc = SyncGlobalization.DataSourceStringComparison;

            var sn = this.SchemaName == null ? string.Empty : this.SchemaName;
            var otherSn = other.SchemaName == null ? string.Empty : other.SchemaName;

            return string.Equals(this.TableName, other.TableName, sc)
                && string.Equals(this.ColumnName, other.ColumnName, sc)
                && string.Equals(this.ParameterName, other.ParameterName, sc)
                && string.Equals(sn, otherSn, sc);
        }

        public override bool Equals(object obj) => this.Equals(obj as SetupFilterWhere);

        public override int GetHashCode() => base.GetHashCode();

        public static bool operator ==(SetupFilterWhere left, SetupFilterWhere right)
            => EqualityComparer<SetupFilterWhere>.Default.Equals(left, right);

        public static bool operator !=(SetupFilterWhere left, SetupFilterWhere right)
            => !(left == right);

    }
}
