using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync.Setup
{
    [DataContract(Name = "sfw"), Serializable]
    public class SetupFilterWhere
    {
        [DataMember(Name = "tn", IsRequired = true, Order = 1)]
        public string TableName { get; set; }

        [DataMember(Name = "sn", IsRequired = false, EmitDefaultValue =false, Order = 2)]
        public string SchemaName { get; set; }

        [DataMember(Name = "cn", IsRequired = true, Order = 3)]
        public string ColumnName { get; set; }

        [DataMember(Name = "pn", IsRequired = true, Order = 4)]
        public string ParameterName { get; set; }

    }
}
