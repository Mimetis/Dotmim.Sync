
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Builders
{
    public enum DbCommandType 
    {
        None,
        SelectChanges,
        SelectInitializedChanges,
        SelectInitializedChangesWithFilters,
        SelectChangesWithFilters,
        SelectRow,
        UpdateRow,
        InsertRow,
        DeleteRow,
        DisableConstraints,
        EnableConstraints,
        DeleteMetadata,
        UpdateMetadata,
        SelectMetadata,
        InsertTrigger,
        UpdateTrigger,
        DeleteTrigger,
        UpdateRows,
        InsertRows,
        DeleteRows,
        BulkTableType,
        UpdateUntrackedRows,
        Reset,
        PreUpdateRows,
        PreInsertRows,
        PreDeleteRows,
        PreUpdateRow,
        PreInsertRow,
        PreDeleteRow,

    }
}
