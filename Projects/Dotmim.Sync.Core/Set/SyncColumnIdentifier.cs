using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    [DataContract(Name = "sci"), Serializable]
    public class SyncColumnIdentifier
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

    }
}
