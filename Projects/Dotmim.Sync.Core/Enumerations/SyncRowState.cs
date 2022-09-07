using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Enumerations
{
    /// <summary>
    /// Gets the state of a SyncRow object.
    /// </summary>
    [Flags]
    public enum SyncRowState
    {
        /// <summary>
        /// The row has no state yet
        /// </summary>
        None = 2,

        /// <summary>
        /// The row is mark as deleted
        /// </summary>
        Deleted = 8,

        /// <summary>
        /// The row is mark as modified (update or insert)
        /// </summary>
        Modified = 16,

        /// <summary>
        /// The row is mark as to be retry on next sync as a deleted row
        /// </summary>
        RetryDeletedOnNextSync = 32,

        /// <summary>
        /// The row is mark as to be retry on next sync as a modified row
        /// </summary>
        RetryModifiedOnNextSync = 64,

        /// <summary>
        /// The row is mark as failed on apply as deleted
        /// </summary>
        ApplyDeletedFailed = 128,

        /// <summary>
        /// The row is mark as failed on apply as modified
        /// </summary>
        ApplyModifiedFailed = 256,

    }
}
