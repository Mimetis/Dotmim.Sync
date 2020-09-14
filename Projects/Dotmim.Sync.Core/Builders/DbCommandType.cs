
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
        UpdateBatchRows,
        DeleteRow,
        DeleteBatchRows,
        DisableConstraints,
        EnableConstraints,
        DeleteMetadata,
        UpdateMetadata,
        UpdateUntrackedRows,
        Reset
    }
}
