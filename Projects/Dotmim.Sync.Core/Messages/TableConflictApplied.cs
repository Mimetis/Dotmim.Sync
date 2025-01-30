using System;

namespace Dotmim.Sync
{
    /// <summary>
    /// Table Conflict Error Applied.
    /// </summary>
    internal class TableConflictErrorApplied
    {
        /// <summary>
        /// Gets or sets a value indicating whether gets or Sets result of conflict resolution.
        /// </summary>
        public bool HasBeenResolved { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether gets or Sets a row to be mark for next sync again.
        /// </summary>
        public bool HasBeenMarkForNextSync { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether gets or Sets a boolean indicating if the conflict resolution has been applied locally.
        /// </summary>
        public bool HasBeenApplied { get; set; }

        /// <summary>
        /// Gets or Sets the Exception if an error occured during conflict resolution.
        /// </summary>
        public Exception Exception { get; set; }

        /// <summary>
        /// Gets or Sets the row in error / conflict.
        /// </summary>
        public SyncRow Row { get; set; }
    }
}