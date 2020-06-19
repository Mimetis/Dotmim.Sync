
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Builders
{
    public enum DbCommandType
    {
        SelectChanges,
        SelectInitializedChanges,
        SelectInitializedChangesWithFilters,
        SelectChangesWithFilters,
        SelectRow,
        UpdateRow,
        DeleteRow,
        DisableConstraints,
        EnableConstraints,
        DeleteMetadata,
        UpdateMetadata,
        InsertTrigger,
        UpdateTrigger,
        DeleteTrigger,
        BulkTableType,
        BulkUpdateRows,
        BulkDeleteRows,
        UpdateUntrackedRows,
        Reset
    }
}
