using Dotmim.Sync.Enumerations;
using System;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{


    /// <summary>
    /// Mapping sur la table ScopeInfo
    /// </summary>
    [DataContract(Name = "scope"), Serializable]
    public class ScopeInfo
    {
        /// <summary>
        /// For serialization purpose
        /// </summary>
        public ScopeInfo()
        {

        }
        /// <summary>
        /// Scope name. Shared by all clients and the server
        /// </summary>
        [DataMember(Name = "n", IsRequired = true, Order = 1)]
        public string Name { get; set; }

        /// <summary>
        /// Scope schema. stored locally on the client
        /// </summary>
        [DataMember(Name = "sch", IsRequired = true, Order = 2)]
        public SyncSet Schema { get; set; }

        /// <summary>
        /// Setup. stored locally on the client
        /// </summary>
        [DataMember(Name = "s", IsRequired = true, Order = 3)]
        public SyncSetup Setup { get; set; }

        /// <summary>
        /// Gets or Sets the schema version
        /// </summary>
        [DataMember(Name = "v", IsRequired = false, EmitDefaultValue = false, Order = 4)]
        public string Version { get; set; }

        /// <summary>
        /// Gets or Sets the last timestamp a sync has occured. This timestamp is set just 'before' sync start.
        /// </summary>
        [DataMember(Name = "lst", IsRequired = false, EmitDefaultValue = false, Order = 5)]
        public long? LastCleanupTimestamp { get; set; }

        /// <summary>
        /// Gets or Sets the additional properties. 
        /// </summary>
        [IgnoreDataMember]
        public string Properties { get; set; }

    }
}
