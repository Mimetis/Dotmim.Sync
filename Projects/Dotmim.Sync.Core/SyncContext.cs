using Dotmim.Sync.Enumerations;
using System;

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
        /// Current Session, in progress
        /// </summary>
        [DataMember(Name = "id", IsRequired = true, Order = 1)]
        public Guid SessionId { get; set; }

       /// <summary>
        /// Gets or Sets the ScopeName for this sync session
        /// </summary>
        [DataMember(Name = "sn", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public string ScopeName { get; set; }

        /// <summary>
        /// Gets or sets the sync type used during this session. Can be : Normal, Reinitialize, ReinitializeWithUpload
        /// </summary>
        [DataMember(Name = "typ", IsRequired = false, EmitDefaultValue = false, Order = 5)]
        public SyncType SyncType { get; set; }

        /// <summary>
        /// Gets or Sets the current Sync direction. 
        /// When locally GetChanges and remote ApplyChanges, we are in Upload direction
        /// When remote GetChanges and locally ApplyChanges, we are in Download direction
        /// this Property is used to check SyncDirection on each table.
        /// </summary>
        [DataMember(Name = "way", IsRequired = false, EmitDefaultValue = false, Order = 6)]
        public SyncWay SyncWay { get; set; }

        /// <summary>
        /// Actual sync stage
        /// </summary>
        [DataMember(Name = "stage", IsRequired = false, EmitDefaultValue = false, Order = 11)]
        public SyncStage SyncStage { get; set; }

        /// <summary>
        /// Get or Sets the Sync parameter to pass to Remote provider for filtering rows
        /// </summary>
        [DataMember(Name = "ps", IsRequired = false, EmitDefaultValue = false, Order = 12)]
        public SyncParameters Parameters { get; set; }

        /// <summary>
        /// Ctor. New sync context with a new Guid
        /// </summary>
        public SyncContext(Guid sessionId, string scopeName)
        {
            this.SessionId = sessionId;
            this.ScopeName = scopeName;
        }

        /// <summary>
        /// Used for serialization purpose
        /// </summary>
        public SyncContext()
        {

        }

        /// <summary>
        /// Get the result if sync session is ended
        /// </summary>
        public override string ToString() => this.ScopeName;
    }
}
