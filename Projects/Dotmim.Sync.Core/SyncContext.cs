using Dotmim.Sync.Enumerations;
using System;
using Dotmim.Sync.Filter;

namespace Dotmim.Sync
{
    /// <summary>
    /// Context of the current Sync session
    /// Encapsulates data changes and metadata for a synchronization session.
    /// </summary>
    [Serializable]
    public class SyncContext
    {
        /// <summary>
        /// Current Session, in progress
        /// </summary>
        public Guid SessionId { get; set; }

        /// <summary>Gets or sets the time when a sync sessionn started.
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// <summary>Gets or sets the time when a sync session ended.
        /// </summary>
        public DateTime CompleteTime { get; set; }

        /// <summary>
        /// Gets or sets the sync type used during this session. Can be : Normal, Reinitialize, ReinitializeWithUpload
        /// </summary>
        public SyncType SyncType { get; set; }

        /// <summary>
        /// Gets or Sets the current Sync direction. 
        /// When locally GetChanges and remote ApplyChanges, we are in Upload direction
        /// When remote GetChanges and locally ApplyChanges, we are in Download direction
        /// this Property is used to check SyncDirection on each table.
        /// </summary>
        public SyncWay SyncWay { get; set; }

        /// <summary>
        /// Total number of change sets downloaded
        /// </summary>
        public int TotalChangesDownloaded { get; set; }

        /// <summary>
        /// Total number of change sets uploaded
        /// </summary>
        public int TotalChangesUploaded { get; set; }

        /// <summary>
        /// Total number of Sync Conflicts
        /// </summary>
        public int TotalSyncConflicts { get; set; }

        /// <summary>
        /// Total number of Sync Conflicts
        /// </summary>
        public int TotalSyncErrors { get; set; }

        /// <summary>
        /// Actual sync stage
        /// </summary>
        public SyncStage SyncStage { get; set; }

        /// <summary>
        /// Get or Sets the Sync parameter to pass to Remote provider for filtering rows
        /// </summary>
        public SyncParameterCollection Parameters { get; set; }


        public SyncContext(Guid sessionId)
        {
            this.SessionId = sessionId;
        }

     

    }
}
