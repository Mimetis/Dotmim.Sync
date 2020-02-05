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
        ScopeLoading,
        ScopeSaved,

        SnapshotApplying,
        SnapshotApplied,

        SchemaReading,
        SchemaRead,
        SchemaApplying,
        SchemaApplied,
        TableSchemaApplying,
        TableSchemaApplied,

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