using Dotmim.Sync.Enumerations;
using System;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// Mapping sur la table ScopeInfo
    /// </summary>
    [DataContract(Name = "server_scope"), Serializable]
    public class ServerScopeInfo : IScopeInfo
    {
        /// <summary>
        /// Scope name. Shared by all clients and the server
        /// </summary>
        [DataMember(Name = "n", IsRequired = true, Order = 1)]
        public string Name { get; set; }

        /// <summary>
        /// Scope schema. stored locally
        /// </summary>
        [DataMember(Name = "sch", IsRequired = true, Order = 2)]
        public SyncSet Schema { get; set; }

        /// <summary>
        /// Gets or Sets if this server scope has just been created now in server database.
        /// We don't need it on the client side, so ignore it
        /// </summary>
        [DataMember(Name = "new", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public bool IsNewScope { get; set; }

        /// <summary>
        /// Setup. stored locally
        /// </summary>
        [DataMember(Name = "s", IsRequired = true, Order = 4)]
        public SyncSetup Setup { get; set; }

        /// <summary>
        /// Gets or Sets the schema version
        /// </summary>
        [DataMember(Name = "v", IsRequired = false, EmitDefaultValue = false, Order = 5)]
        public string Version { get; set; }

        /// <summary>
        /// Gets or Sets the last timestamp a sync has occured. This timestamp is set just 'before' sync start.
        /// </summary>
        [DataMember(Name = "lst", IsRequired = false, EmitDefaultValue = false, Order = 6)]
        public long LastCleanupTimestamp { get; set; }


        /// <summary>
        /// Gets or Sets the order to execute on the client
        /// </summary>
        [DataMember(Name = "oo", IsRequired = false, EmitDefaultValue = false, Order = 7)]
        public SyncOrder OverrideOrder { get; set; }
    }
}
