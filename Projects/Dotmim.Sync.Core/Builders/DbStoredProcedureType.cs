using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Builders
{
    public enum DbStoredProcedureType
    {
        SelectChanges,
        SelectChangesWithFilters,
        SelectInitializedChanges,
        SelectInitializedChangesWithFilters,
        SelectRow,
        UpdateRow,
        DeleteRow,
        BulkUpdateRows,
        BulkDeleteRows,
        Reset,
        BulkTableType,
    }
}
