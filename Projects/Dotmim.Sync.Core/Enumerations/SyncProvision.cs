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
        Table = 1,
        TrackingTable = 2,
        StoredProcedures = 4,
        Triggers = 8,
        ClientScope = 16,
        ServerScope = 32,
        ServerHistoryScope = 64,
    }
}
