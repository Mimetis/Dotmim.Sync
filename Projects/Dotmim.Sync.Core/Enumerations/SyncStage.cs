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
        /// <summary>
        /// Reading local table metadata.
        /// </summary>
        ReadingMetadata,

        /// <summary>
        /// Reading the table schema from the store.
        /// </summary>
        ReadingSchema,

        /// <summary>
        /// Updating local metadata.
        /// </summary>
        WritingMetadata,

        /// <summary>Applying inserts to the store.</summary>
        ApplyingInserts,
        /// <summary>Applying updates to the store.</summary>
        ApplyingUpdates,
        /// <summary>Applying deletes to the store.</summary>
        ApplyingDeletes,
        /// <summary>Applying changes to the store.</summary>
        ApplyingChanges,

        /// <summary>Enumerating changes from the store.</summary>
        SelectedChanges,

        /// <summary>Cleanup metadata from tracking tables.</summary>
        CleanupMetadata
    }
}