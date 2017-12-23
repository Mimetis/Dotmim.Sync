using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Enumerations
{
    [Flags]
    public enum SyncProvision
    {
        Table = 1,
        Triggers = 2,
        StoredProcedures = 4,
        TrackingTable = 8,
        All = 16
    }
}
