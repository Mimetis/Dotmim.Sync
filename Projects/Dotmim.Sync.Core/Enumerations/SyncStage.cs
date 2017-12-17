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
        /// <summary>No Sync Stage involved</summary>
        None,

        BeginSession,

        ScopeLoading,
        ScopeSaved,

        ConfigurationApplying,
        ConfigurationApplied,

        DatabaseApplying,
        DatabaseApplied,

        DatabaseTableApplying,
        DatabaseTableApplied,

        ChangesSelecting,
        ChangesSelected,

        ChangesApplying,
        ChangesApplied,

        EndSession,

        /// <summary>Cleanup metadata from tracking tables.</summary>
        CleanupMetadata

    }
}