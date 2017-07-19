using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Core.Context
{
    public class SyncStats
    {
        /// <summary>
        /// Current Session, in progress
        /// </summary>
        public Guid SessionId { get; internal set; }

        /// <summary>
        /// Gets or Sets the Set Schema used for this sync
        /// </summary>
        public DmSet ScopeSet { get; set; }

        /// <summary>
        /// Gets or sets the number of changes downloaded from the server that were applied at the client.
        /// </summary>
        public int DownloadChangesApplied { get; set; }

        /// <summary>
        /// Gets or sets the number of changes downloaded from the server that could not be applied at the client.
        /// </summary>
        public int DownloadChangesFailed { get; set; }

        /// <summary>Gets or sets the time when a sync session ended.
        /// </summary>
        public DateTime SyncCompleteTime { get; set; }

        /// <summary>Gets or sets the time when a sync sessionn started.
        /// </summary>
        public DateTime SyncStartTime { get; set; }

        /// <summary>
        /// Gets or sets the total number of changes downloaded from the server. This includes the changes that could not be applied at the client.
        /// </summary>
        public int TotalChangesDownloaded { get; set; }

        /// <summary>
        /// Gets or sets the total number of changes uploaded from the client. This includes the changes that could not be applied at the server.
        /// </summary>
        public int TotalChangesUploaded { get; set; }

        /// <summary>
        /// Gets or sets the number of changes uploaded from the client that were applied at the server.
        /// </summary>
        public int UploadChangesApplied { get; set; }

        /// <summary>
        /// Gets or sets the number of changes uploaded from the client that could not be applied at the server.
        /// </summary>
        public int UploadChangesFailed { get; set; }

        public SyncStats(Guid sessionId)
        {
            this.SessionId = sessionId;
        }
    }
}
