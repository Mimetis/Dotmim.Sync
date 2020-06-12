using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync.Setup
{
    [DataContract(Name = "sfw"), Serializable]
    public class SetupFilterWhere : SyncNamedItem<SetupFilterWhere>
    {
        [DataMember(Name = "tn", IsRequired = true, Order = 1)]
        public string TableName { get; set; }

        [DataMember(Name = "sn", IsRequired = false, EmitDefaultValue =false, Order = 2)]
        public string SchemaName { get; set; }

        [DataMember(Name = "cn", IsRequired = true, Order = 3)]
        public string ColumnName { get; set; }

        [DataMember(Name = "pn", IsRequired = true, Order = 4)]
        public string ParameterName { get; set; }


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
