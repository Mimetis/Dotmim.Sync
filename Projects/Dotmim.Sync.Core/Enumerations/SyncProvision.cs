using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Enumerations
{
    /// <summary>
    /// Gets the objects we want to provision or deprovision
    /// Be careful, SyncProvision.Table, will (de)provision the data tables !
    /// </summary>
    [Flags]
    public enum SyncProvision
    {
        TrackingTable = 1,
        StoredProcedures = 2,
        Triggers = 4,
        Scope = 8,
        All = 16,
        Table = 32
    }
}
