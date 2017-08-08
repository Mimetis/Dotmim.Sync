using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Builders
{
    public enum DbCommandType
    {
        SelectChanges,
        SelectChangesWitFilters,
        SelectRow,
        InsertRow,
        UpdateRow,
        DeleteRow,
        InsertMetadata,
        UpdateMetadata,
        DeleteMetadata,
        InsertTrigger,
        UpdateTrigger,
        DeleteTrigger,
        BulkTableType,
        BulkInsertRows,
        BulkUpdateRows,
        BulkDeleteRows
    }
}
