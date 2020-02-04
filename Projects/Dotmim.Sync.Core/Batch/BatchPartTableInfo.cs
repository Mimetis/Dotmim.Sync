using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync.Batch
{
    [DataContract(Name = "t"), Serializable]
    public class BatchPartTableInfo : IEquatable<BatchPartTableInfo>
    {
        public BatchPartTableInfo()
        {

        }

        public BatchPartTableInfo(string tableName, string schemaName = null)
        {
            this.TableName = tableName;
            this.SchemaName = schemaName;
        }

        /// <summary>
        /// Gets or sets the name of the table that the DmTableSurrogate object represents.
        /// </summary>
        [DataMember(Name = "n", IsRequired = true, Order = 1)]
        public string TableName { get; set; }

        /// <summary>
        /// Get or Set the schema used for the DmTableSurrogate
        /// </summary>
        [DataMember(Name = "s", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public string SchemaName { get; set; }


        public bool Equals(BatchPartTableInfo other)
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

        public override bool Equals(object obj)
            => this.Equals(obj as SyncTable);

        public override int GetHashCode()
        {
            var hashCode = 1627045777;
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(this.TableName);
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(this.SchemaName);
            return hashCode;
        }

        public static bool operator ==(BatchPartTableInfo left, BatchPartTableInfo right) 
            => EqualityComparer<BatchPartTableInfo>.Default.Equals(left, right);

        public static bool operator !=(BatchPartTableInfo left, BatchPartTableInfo right) 
            => !(left == right);
    }
}
