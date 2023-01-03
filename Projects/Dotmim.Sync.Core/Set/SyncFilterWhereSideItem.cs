using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    [DataContract(Name = "sfwsi"), Serializable]
    public class SyncFilterWhereSideItem : SyncNamedItem<SyncFilterWhereSideItem>
    {

        [DataMember(Name = "c", IsRequired = true, Order = 1)]
        public string ColumnName { get; set; }

        [DataMember(Name = "t", IsRequired = true, Order = 2)]
        public string TableName { get; set; }

        [DataMember(Name = "s", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public string SchemaName { get; set; }

        [DataMember(Name = "p", IsRequired = true, Order = 4)]
        public string ParameterName { get; set; }


        /// <summary>
        /// Gets the ShemaTable's SyncSchema
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; set; }


        /// <summary>
        /// Ensure filter parameter as the correct schema (since the property is not serialized)
        /// </summary>
        public void EnsureFilterWhereSideItem(SyncSet schema) => this.Schema = schema;

        /// <summary>
        /// Get all comparable fields to determine if two instances are identifed as same by name
        /// </summary>
        public override IEnumerable<string> GetAllNamesProperties()
        {
            yield return this.TableName;
            yield return this.SchemaName;
            yield return this.ColumnName;
            yield return this.ParameterName;
        }
    }
}
