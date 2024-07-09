using System;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{

    /// <summary>
    /// Mapping sur la table ScopeInfo.
    /// </summary>
    [DataContract(Name = "scope"), Serializable]
    public class ScopeInfo
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ScopeInfo"/> class.
        /// For serialization purpose.
        /// </summary>
        public ScopeInfo()
        {
        }

        /// <summary>
        /// Gets or sets scope name. Shared by all clients and the server.
        /// </summary>
        [DataMember(Name = "n", IsRequired = true, Order = 1)]
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets scope schema. stored locally on the client.
        /// </summary>
        [DataMember(Name = "sch", IsRequired = true, Order = 2)]
        public SyncSet Schema { get; set; }

        /// <summary>
        /// Gets or sets setup. stored locally on the client.
        /// </summary>
        [DataMember(Name = "s", IsRequired = true, Order = 3)]
        public SyncSetup Setup { get; set; }

        /// <summary>
        /// Gets or Sets the schema version.
        /// </summary>
        [DataMember(Name = "v", IsRequired = false, EmitDefaultValue = false, Order = 4)]
        public string Version { get; set; }

        /// <summary>
        /// Gets or Sets the last timestamp a sync has occured. This timestamp is set just 'before' sync start.
        /// </summary>
        [DataMember(Name = "lst", IsRequired = false, Order = 5)]
        public long? LastCleanupTimestamp { get; set; }

        /// <summary>
        /// Gets or Sets the additional properties.
        /// </summary>
        [IgnoreDataMember]
        public string Properties { get; set; }

        /// <summary>
        /// Get the scope name / last cleanup / setup tables count.
        /// </summary>
        /// <returns></returns>
        public override string ToString() => $"Scope Name:{this.Name}({this.Version}). Last cleanup:{this.LastCleanupTimestamp}. Setup tables:{this.Setup?.Tables?.Count}. Schema tables:{this.Schema?.Tables?.Count}";
    }
}