using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync
{
    public enum SyncInterceptorStep
    {
        None,
        Session,
        Scope,
        Schema,
        ApplyChangesFailed,
        TableProvision,
        TableDeprovision,
        TableChangesApply,
        TableChangesSelect,
        DatabaseProvision,
        DatabaseDeprovision,
        CleanupMetadata,
        Outdated,
    }
}
