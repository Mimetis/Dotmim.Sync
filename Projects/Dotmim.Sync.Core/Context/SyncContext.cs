using Dotmim.Sync.Core.Batch;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dotmim.Sync.Core.Scope;

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

        /// <summary>
        /// Gets or Sets the Set Schema used for this sync
        /// </summary>
        public DmSet ScopeSet { get; set; }

        /// <summary>
        /// Get the Server sync progress
        /// </summary>
        public SyncSetProgress Server { get; } = new SyncSetProgress();

        /// <summary>
        /// Get the Client sync progress
        /// </summary>
        public SyncSetProgress Client { get; } = new SyncSetProgress();

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

        public SyncContext(Guid sessionId)
        {
            this.SessionId = sessionId;
        }
    }
}
