using Dotmim.Sync.Extensions;
using Dotmim.Sync.Serialization;
using System;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// Mapping sur la table ScopeInfo.
    /// </summary>
    [DataContract(Name = "scope_client"), Serializable]
    public class ScopeInfoClient
    {
        private static readonly ISerializer JsonSerializer = SerializersFactory.JsonSerializerFactory.GetSerializer();

        /// <summary>
        /// Initializes a new instance of the <see cref="ScopeInfoClient"/> class.
        /// For serialization purpose.
        /// </summary>
        public ScopeInfoClient()
        {
        }

        /// <summary>
        /// Gets or sets id of the scope owner.
        /// </summary>
        [DataMember(Name = "id", IsRequired = true, Order = 1)]
        public Guid Id { get; set; }

        /// <summary>
        /// Gets or sets scope name. Shared by all clients and the server.
        /// </summary>
        [DataMember(Name = "n", IsRequired = true, Order = 2)]
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets scope Hash: Filters hash or null.
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
        /// Gets or sets a value indicating whether gets or Sets if the client scope is new in the local datasource.
        /// If new, we will override timestamp for first synchronisation to be sure to get all datas from server.
        /// </summary>
        [DataMember(Name = "in", IsRequired = true, Order = 6)]
        public bool IsNewScope { get; set; }

        /// <summary>
        /// Gets or Sets the parameters.
        /// </summary>
        [DataMember(Name = "p", IsRequired = false, Order = 7)]
        public SyncParameters Parameters { get; set; }

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
        /// Gets or Sets the errors batch info occured on last sync.
        /// </summary>
        [IgnoreDataMember]
        public string Errors { get; set; }

        /// <summary>
        /// Gets a readable version of LastSyncDuration.
        /// </summary>
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
        /// Make a shadow copy of an old scope to get the last sync information copied on this scope.
        /// </summary>
        public void ShadowScope(ScopeInfoClient oldScopeInfoClient)
        {
            Guard.ThrowIfNull(oldScopeInfoClient);

            this.LastServerSyncTimestamp = oldScopeInfoClient.LastServerSyncTimestamp;
            this.LastSyncTimestamp = oldScopeInfoClient.LastSyncTimestamp;
            this.LastSync = oldScopeInfoClient.LastSync;
            this.LastSyncDuration = oldScopeInfoClient.LastSyncDuration;
        }

        /// <summary>
        /// Gets the scope info as a string.
        /// </summary>
        public override string ToString()
        {
            var p = this.Parameters != null ? JsonSerializer.Serialize(this.Parameters).ToUtf8String() : null;

            return $"Scope Name:{this.Name}. Id:{this.Id}. Hash:{this.Hash}. LastSyncTimestamp:{this.LastSyncTimestamp}. LastServerSyncTimestamp:{this.LastServerSyncTimestamp}. LastSync:{this.LastSync}. Parameters:{p} ";
        }
    }
}