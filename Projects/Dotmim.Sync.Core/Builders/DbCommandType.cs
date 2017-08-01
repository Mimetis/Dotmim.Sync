using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Core.Builders
{
    public enum DbCommandType
    {
        SelectChanges,
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
