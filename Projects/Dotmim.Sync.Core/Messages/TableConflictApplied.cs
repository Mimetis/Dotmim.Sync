using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync
{
    internal class TableConflictErrorApplied
    {
        /// <summary>
        /// Gets or Sets result of conflict resolution.
        /// </summary>
        public bool HasBeenResolved { get; set; } = false;

        /// <summary>
        /// Gets or Sets a boolean indicating if the conflict resolution has been applied locally
        /// </summary>
        public bool HasBeenApplied { get; set; } = false;

        /// <summary>
        /// Gets or Sets the Exception if an error occured during conflict resolution
        /// </summary>
        public Exception Exception { get; set; }

        /// <summary>
        /// Gets or Sets the row in error / conflict
        /// </summary>
        public SyncRow Row { get; set; }
    }
}
