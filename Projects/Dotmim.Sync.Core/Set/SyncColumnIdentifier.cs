using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    [DataContract(Name = "sci"), Serializable]
    public class SyncColumnIdentifier: SyncNamedItem<SyncColumnIdentifier>
    {
        [DataMember(Name = "c", IsRequired = true, Order = 1)]
        public string ColumnName { get; set; }

        [DataMember(Name = "t", IsRequired = true, Order = 2)]
        public string TableName { get; set; }

        [DataMember(Name = "s", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public string SchemaName { get; set; }

        public SyncColumnIdentifier()
        {

        }

        public SyncColumnIdentifier(string columnName, string tableName, string schemaName = null)
        {
            this.TableName = tableName;
            this.SchemaName = schemaName;
            this.ColumnName = columnName;
        }

 
        public SyncColumnIdentifier Clone() => new SyncColumnIdentifier
        {
            ColumnName = this.ColumnName,
            SchemaName = this.SchemaName,
            TableName = this.TableName
        };

        public override string ToString()
        {
            if (string.IsNullOrEmpty(SchemaName))
                return $"{TableName}-{ColumnName}";
            else
                return $"{SchemaName}.{TableName}-{ColumnName}";

        }

        public override IEnumerable<string> GetAllNamesProperties()
        {
            yield return this.TableName;
            yield return this.SchemaName;
            yield return this.ColumnName;
        }
       
    }
}
