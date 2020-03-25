using Dotmim.Sync.Enumerations;
using System;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// Mapping sur la table ScopeInfo
    /// </summary>
    [DataContract(Name = "server_scope"), Serializable]
    public class ServerScopeInfo
    {
        /// <summary>
        /// Scope name. Shared by all clients and the server
        /// </summary>
        [DataMember(Name = "n", IsRequired = true, Order = 1)]
        public string Name { get; set; }

        /// <summary>
        /// Scope schema. stored locally on the client
        /// When returning from server we don't want it in the payload.
        /// </summary>
        [IgnoreDataMember]
        public string Schema { get; set; }

        /// <summary>
        /// Gets or Sets the schema version
        /// </summary>
        [DataMember(Name = "v", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public string Version { get; set; }

        /// <summary>
        /// Gets or Sets the last timestamp a sync has occured. This timestamp is set just 'before' sync start.
        /// </summary>
        [DataMember(Name = "lst", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public long LastCleanupTimestamp { get; set; }

    }
}
