using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    [DataContract(Name = "sci"), Serializable]
    public class SyncColumnIdentifier: IEquatable<SyncColumnIdentifier>
    {
        [DataMember(Name = "c", IsRequired = true, Order = 1)]
        public String ColumnName { get; set; }

        [DataMember(Name = "t", IsRequired = true, Order = 2)]
        public String TableName { get; set; }

        [DataMember(Name = "s", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public String SchemaName { get; set; }

        public SyncColumnIdentifier()
        {

        }

        public SyncColumnIdentifier(string columnName, string tableName, string schemaName = null)
        {
            this.TableName = tableName;
            this.SchemaName = schemaName;
            this.ColumnName = columnName;
        }

        public SyncColumnIdentifier Clone()
        {
            return new SyncColumnIdentifier
            {
                ColumnName = this.ColumnName,
                SchemaName = this.SchemaName,
                TableName = this.TableName
            };
        }

        public override string ToString()
        {
            if (String.IsNullOrEmpty(SchemaName))
                return $"{TableName}-{ColumnName}";
            else
                return $"{SchemaName}.{TableName}-{ColumnName}";

        }

        public bool Equals(SyncColumnIdentifier other)
        {
            if (other == null)
                return false;

            var sc = SyncGlobalization.DataSourceStringComparison;

            var sn = this.SchemaName == null ? string.Empty : this.SchemaName;
            var otherSn = other.SchemaName == null ? string.Empty : other.SchemaName;

            return this.ColumnName.Equals(other.ColumnName, sc) &&
                   this.TableName.Equals(other.TableName, sc) &&
                   sn.Equals(otherSn, sc);
        }

        public override bool Equals(object obj) => this.Equals(obj as SyncColumnIdentifier);

        public override int GetHashCode() => base.GetHashCode();

        public static bool operator ==(SyncColumnIdentifier left, SyncColumnIdentifier right) 
            => EqualityComparer<SyncColumnIdentifier>.Default.Equals(left, right);

        public static bool operator !=(SyncColumnIdentifier left, SyncColumnIdentifier right) 
            => !(left == right);
    }
}
