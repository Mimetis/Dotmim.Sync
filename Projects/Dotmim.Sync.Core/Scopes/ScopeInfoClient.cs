using Dotmim.Sync.Enumerations;
using System;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// Mapping sur la table ScopeInfo
    /// </summary>
    [DataContract(Name = "scope_client"), Serializable]
    public class ScopeInfoClient
    {
        /// <summary>
        /// Id of the scope owner
        /// </summary>
        [DataMember(Name = "id", IsRequired = true, Order = 1)]
        public Guid Id { get; set; }

        /// <summary>
        /// Scope name. Shared by all clients and the server
        /// </summary>
        [DataMember(Name = "n", IsRequired = true, Order = 2)]
        public string Name { get; set; }

        /// <summary>
        /// Scope Hash: Filters hash or null
        /// </summary>
        [DataMember(Name = "h", IsRequired = true, Order = 3)]
        public string Hash { get; set; }

        /// <summary>
        /// Gets or Sets the last timestamp a sync has occured. This timestamp is set just 'before' sync start.
        /// </summary>
        [DataMember(Name = "lst", IsRequired = true, Order = 4)]
        public long? LastSyncTimestamp { get; set; }

        /// <summary>
        /// Gets or Sets the last server timestamp a sync has occured for this scope client.
        /// </summary>
        [DataMember(Name = "lsst", IsRequired = true, Order = 5)]
        public long? LastServerSyncTimestamp { get; set; }

        /// <summary>
        /// Gets or Sets if the client scope is new in the local datasource.
        /// If new, we will override timestamp for first synchronisation to be sure to get all datas from server
        /// </summary>
        [DataMember(Name = "in", IsRequired = true, Order = 6)]
        public bool IsNewScope { get; set; }

        /// <summary>
        /// Gets or Sets the last datetime when a sync has successfully ended.
        /// </summary>
        [IgnoreDataMember]
        public DateTime? LastSync { get; set; }


        /// <summary>
        /// Gets or Sets the last duration a sync has occured. 
        /// </summary>
        [IgnoreDataMember]
        public long LastSyncDuration { get; set; }


        /// <summary>
        /// Gets or Sets the additional properties. 
        /// </summary>
        [IgnoreDataMember]
        public string Properties { get; set; }


        /// <summary>
        /// Gets a readable version of LastSyncDuration
        /// </summary>
        /// <returns></returns>
        [IgnoreDataMember]
        public string LastSyncDurationString
        {
            get
            {
                var durationTs = new TimeSpan(this.LastSyncDuration);
                return $"{durationTs.Hours}:{durationTs.Minutes}:{durationTs.Seconds}.{durationTs.Milliseconds}";
            }
        }


        /// <summary>
        /// Make a shadow copy of an old scope to get the last sync information copied on this scope
        /// </summary>
        /// <param name="oldClientScopeInfo">old client scope that we we copy infos</param>
        public void ShadowScope(ScopeInfoClient oldClientHistoryScopeInfo)
        {
            this.LastServerSyncTimestamp = oldClientHistoryScopeInfo.LastServerSyncTimestamp;
            this.LastSyncTimestamp = oldClientHistoryScopeInfo.LastSyncTimestamp;
            this.LastSync = oldClientHistoryScopeInfo.LastSync;
            this.LastSyncDuration = oldClientHistoryScopeInfo.LastSyncDuration;
        }

    }
}
