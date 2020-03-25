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
        None,

        BeginSession,
        EndSession,

        ScopeLoading,
        ScopeLoaded,

        SnapshotApplying,
        SnapshotApplied,

        SchemaReading,
        SchemaRead,

        SchemaProvisioning,
        SchemaProvisioned,

        SchemaDeprovisioning,
        SchemaDeprovisioned,

        DatabaseChangesSelecting,
        DatabaseChangesSelected,

        DatabaseChangesApplying,
        DatabaseChangesApplied,

        MetadataCleaning,
        MetadataCleaned,
    }
}