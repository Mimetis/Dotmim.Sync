using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Core
{
    public class OutdatedEventArgs : EventArgs
    {
        /// <summary>
        /// Gets or sets an action enumeration value for the action to handle the outdated peer.
        /// </summary>
        public OutdatedSyncAction Action { get; set; } = OutdatedSyncAction.AbortSync;
    }

    public enum OutdatedSyncAction
    {
        /// <summary>Continue to synchronize, but write any issues to the metadata.</summary>
        PartialSync,
        /// <summary>Reject the synchronization request.</summary>
        AbortSync
    }
}
