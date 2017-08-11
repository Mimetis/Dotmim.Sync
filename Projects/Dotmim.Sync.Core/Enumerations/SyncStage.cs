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

        /// <summary>Occurs after changes are selected from datastore</summary>
        SelectedChanges,

        /// <summary>Occurs before applying changes</summary>
        ApplyingChanges,

        /// <summary>Occurs afeter changes are applied</summary>
        AppliedChanges,

        /// <summary>Occurs before applying inserts </summary>
        ApplyingInserts,
        /// <summary>Occurs before applying updates </summary>
        ApplyingUpdates,
        /// <summary>Occurs before applying deletes </summary>
        ApplyingDeletes,

        /// <summary>Writes scopes</summary>
        WriteMetadata,

        /// <summary>End the current sync session</summary>
        EndSession,

        /// <summary>Cleanup metadata from tracking tables.</summary>
        CleanupMetadata

    }
}