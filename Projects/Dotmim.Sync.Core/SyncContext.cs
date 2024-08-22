using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// Context of the current Sync session
    /// Encapsulates data changes and metadata for a synchronization session.
    /// </summary>
    [DataContract(Name = "ctx"), Serializable]
    public class SyncContext
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SyncContext"/> class.
        /// </summary>
        public SyncContext(Guid sessionId, string scopeName, SyncParameters parameters = default)
        {
            this.SessionId = sessionId;
            this.ScopeName = scopeName;
            this.Parameters = parameters;
            this.StartTime = DateTime.UtcNow;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncContext"/> class using scope name, parameters and client id from the scope info client.
        /// </summary>
        public SyncContext(Guid sessionId, ScopeInfoClient scopeInfoClient)
        {
            Guard.ThrowIfNull(scopeInfoClient);

            this.SessionId = sessionId;
            this.ScopeName = scopeInfoClient.Name;
            this.Parameters = scopeInfoClient.Parameters;
            this.ClientId = scopeInfoClient.Id;
            this.StartTime = DateTime.UtcNow;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncContext"/> class.
        /// Used for serialization purpose.
        /// </summary>
        public SyncContext() => this.StartTime = DateTime.UtcNow;

        /// <summary>
        /// Gets or sets get or sets the current Session id, in progress.
        /// </summary>
        [DataMember(Name = "id", IsRequired = true, Order = 1)]
        public Guid SessionId { get; set; }

        /// <summary>
        /// Gets or sets current Scope Info Id, in progress.
        /// </summary>
        [DataMember(Name = "csid", IsRequired = true, Order = 2)]
        public Guid? ClientId { get; set; }

        /// <summary>
        /// Gets or Sets the ScopeName for this sync session.
        /// </summary>
        [DataMember(Name = "sn", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public string ScopeName { get; set; }

        /// <summary>
        /// Gets the time when a sync session started.
        /// </summary>
        [IgnoreDataMember]
        public DateTime StartTime { get; }

        /// <summary>
        /// Gets or sets the sync type used during this session. Can be : Normal, Reinitialize, ReinitializeWithUpload.
        /// </summary>
        [DataMember(Name = "typ", IsRequired = false, EmitDefaultValue = false, Order = 4)]
        public SyncType SyncType { get; set; }

        /// <summary>
        /// Gets or Sets the current Sync direction.
        /// When locally GetChanges and remote ApplyChanges, we are in Upload direction
        /// When remote GetChanges and locally ApplyChanges, we are in Download direction
        /// this Property is used to check SyncDirection on each table.
        /// </summary>
        [DataMember(Name = "way", IsRequired = false, EmitDefaultValue = false, Order = 5)]
        public SyncWay SyncWay { get; set; }

        /// <summary>
        /// Gets or sets actual sync stage.
        /// </summary>
        [DataMember(Name = "stage", IsRequired = false, EmitDefaultValue = false, Order = 6)]
        public SyncStage SyncStage { get; set; }

        /// <summary>
        /// Gets or sets get or Sets the Sync parameter to pass to Remote provider for filtering rows.
        /// </summary>
        [DataMember(Name = "ps", IsRequired = false, EmitDefaultValue = false, Order = 7)]
        public SyncParameters Parameters { get; set; }

        /// <summary>
        /// Gets get or Sets the Sync parameter to pass to Remote provider for filtering rows.
        /// </summary>
        [IgnoreDataMember]
        public string Hash
        {
            get
            {
                if (this.Parameters == null || this.Parameters.Count <= 0)
                    return SyncParameters.DefaultScopeHash;
                else
                    return this.Parameters.GetHash();
            }
        }

        /// <summary>
        /// Gets or sets get or Sets additional properties you want to use.
        /// </summary>
        [DataMember(Name = "ap", IsRequired = false, EmitDefaultValue = false, Order = 8)]
        public Dictionary<string, string> AdditionalProperties { get; set; }

        /// <summary>
        /// Gets or Sets the current percentage progress overall.
        /// </summary>
        [DataMember(Name = "pp", IsRequired = false, Order = 9)]
        public double ProgressPercentage { get; set; }

        /// <summary>
        /// Copy local properties to another syncContext instance.
        /// </summary>
        public void CopyTo(SyncContext otherSyncContext)
        {
            Guard.ThrowIfNull(otherSyncContext);

            otherSyncContext.Parameters = this.Parameters;
            otherSyncContext.ScopeName = this.ScopeName;
            otherSyncContext.ClientId = this.ClientId;
            otherSyncContext.SessionId = this.SessionId;
            otherSyncContext.SyncStage = this.SyncStage;
            otherSyncContext.SyncType = this.SyncType;
            otherSyncContext.SyncWay = this.SyncWay;
            otherSyncContext.ProgressPercentage = this.ProgressPercentage;

            if (this.AdditionalProperties != null)
            {
                otherSyncContext.AdditionalProperties = new Dictionary<string, string>();
                foreach (var p in this.AdditionalProperties)
                    otherSyncContext.AdditionalProperties.Add(p.Key, p.Value);
            }
        }

        /// <summary>
        /// Get the result if sync session is ended.
        /// </summary>
        public override string ToString() => this.ScopeName;
    }
}