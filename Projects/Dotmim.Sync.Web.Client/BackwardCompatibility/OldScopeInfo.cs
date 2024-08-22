using System;
using System.Runtime.Serialization;

namespace Dotmim.Sync.Web.Client.BackwardCompatibility
{

    /// <summary>
    /// OldScopeInfo is used to serialize the scope info when we are using the old serialization format.
    /// </summary>
    [DataContract(Name = "scope"), Serializable]
    public class OldScopeInfo
    {

        /// <inheritdoc cref="OldScopeInfo"/>
        public OldScopeInfo()
        {
        }

        /// <summary>
        /// Gets or sets scope name. Shared by all clients and the server.
        /// </summary>
        [DataMember(Name = "n", IsRequired = true, Order = 1)]
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets id of the scope owner.
        /// </summary>
        [DataMember(Name = "id", IsRequired = true, Order = 2)]
        public Guid Id { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether gets or Sets if the current provider is newly created one in database.
        /// If new, we will override timestamp for first synchronisation to be sure to get all datas from server.
        /// </summary>
        [DataMember(Name = "in", IsRequired = true, Order = 3)]
        public bool IsNewScope { get; set; }

        /// <summary>
        /// Gets or Sets the schema version.
        /// </summary>
        [DataMember(Name = "v", IsRequired = false, EmitDefaultValue = false, Order = 4)]
        public string Version { get; set; }

        /// <summary>
        /// Gets or Sets the last datetime when a sync has successfully ended.
        /// </summary>
        [IgnoreDataMember]
        public DateTime? LastSync { get; set; }

        /// <summary>
        /// Gets or Sets the last timestamp a sync has occured. This timestamp is set just 'before' sync start.
        /// </summary>
        [DataMember(Name = "lst", IsRequired = false, EmitDefaultValue = false, Order = 5)]
        public long? LastSyncTimestamp { get; set; }

        /// <summary>
        /// Gets or Sets the last server timestamp a sync has occured for this scope client.
        /// </summary>
        [DataMember(Name = "lsst", IsRequired = false, EmitDefaultValue = false, Order = 6)]
        public long? LastServerSyncTimestamp { get; set; }
    }
}