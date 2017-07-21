using Dotmim.Sync.Core.Batch;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dotmim.Sync.Core.Scope;
using Newtonsoft.Json;
using System.ComponentModel;

namespace Dotmim.Sync.Core
{
    /// <summary>
    /// Context of the current Sync session
    /// Encapsulates data changes and metadata for a synchronization session.
    /// </summary>
    public class SyncContext
    {
        /// <summary>
        /// Current Session, in progress
        /// </summary>
        public Guid SessionId { get; internal set; }

        /// <summary>Gets or sets the time when a sync sessionn started.
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// <summary>Gets or sets the time when a sync session ended.
        /// </summary>
        public DateTime CompleteTime { get; set; }

        /// <summary>
        /// Total number of change sets downloaded
        /// </summary>
        public int TotalChangesDownloaded { get; internal set; }

        /// <summary>
        /// Total number of change sets uploaded
        /// </summary>
        public int TotalChangesUploaded { get; internal set; }

        /// <summary>
        /// Total number of Sync Conflicts
        /// </summary>
        public int TotalSyncConflicts { get; internal set; }

        /// <summary>
        /// Total number of Sync Conflicts
        /// </summary>
        public int TotalSyncErrors { get; internal set; }

        /// <summary>
        /// Actual sync stage
        /// </summary>
        public SyncStage SyncStage { get; set; }

        /// <summary>
        /// Error has occured a sync session
        /// </summary>
        public SyncException Error { get; internal set; }

        public SyncContext(Guid sessionId)
        {
            this.SessionId = sessionId;
        }

     

    }
}
