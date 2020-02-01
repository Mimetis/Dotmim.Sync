using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    [DataContract(Name = "sfwsi"), Serializable]
    public class SyncFilterWhereSideItem : SyncColumnIdentifier
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
    }
}
