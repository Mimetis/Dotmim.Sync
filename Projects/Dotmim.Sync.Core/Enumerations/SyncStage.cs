using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Enumerations
{
    /// <summary>
    /// Sync progress step. Used for the user feedback
    /// </summary>
    public enum SyncStage
    {
        /// <summary>Begin a new sync session</summary>
        BeginSession,

        /// <summary>Ensure scopes, configuration and tables</summary>
        EnsureMetadata,

        /// <summary>Enumerating changes </summary>
        SelectedChanges,

        /// <summary>Applying changes </summary>
        ApplyingChanges,

        /// <summary>Applying inserts </summary>
        ApplyingInserts,
        /// <summary>Applying updates </summary>
        ApplyingUpdates,
        /// <summary>Applying deletes </summary>
        ApplyingDeletes,

        /// <summary>Writes scopes</summary>
        WriteMetadata,

        /// <summary>End the current sync session</summary>
        EndSession,

        /// <summary>Cleanup metadata from tracking tables.</summary>
        CleanupMetadata

    }
}