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
        None = 0,

        BeginSession,
        EndSession,

        ScopeLoading,
        ScopeWriting,

        SnapshotCreating,
        SnapshotApplying,

        Provisioning,
        Deprovisioning,

        ChangesSelecting,
        ChangesApplying,

        Migrating,

        MetadataCleaning,
    }
}