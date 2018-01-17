using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public class OutdatedEventArgs : EventArgs
    {
        /// <summary>
        /// Gets or sets an action enumeration value for the action to handle the outdated peer.
        /// </summary>
        public OutdatedSyncAction Action { get; set; } = OutdatedSyncAction.Rollback;
    }

    public enum OutdatedSyncAction
    {
        /// <summary>
        /// Reinitialize the whole sync database, applying all rows from the server to the client
        /// </summary>
        Reinitialize,
        /// <summary>
        /// Reinitialize the whole sync database, applying all rows from the server to the client, after trying a client upload
        /// </summary>
        ReinitializeWithUpload,
        /// <summary>
        /// Rollback the synchronization request.
        /// </summary>
        Rollback
    }
}
