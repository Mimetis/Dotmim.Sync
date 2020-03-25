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

        ScopeCreating,
        ScopeLoading,
        ScopeLoaded,
        ScopeSaved,

        SnapshotApplying,
        SnapshotApplied,

        SchemaReading,
        SchemaRead,

        SchemaProvisioning,
        SchemaProvisioned,
        TableSchemaProvisioning,
        TableSchemaProvisioned,

        SchemaDeprovisioning,
        SchemaDeprovisioned,
        TableSchemaDeprovisioning,
        TableSchemaDeprovisioned,

        TableChangesSelecting,
        TableChangesSelected,

        DatabaseChangesApplying,
        DatabaseChangesApplied,
        TableChangesApplying,
        TableChangesApplied,

        EndSession,
        CleanupMetadata
    }
}