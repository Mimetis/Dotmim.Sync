using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Enumerations
{

    /// <summary>
    /// Basic mode : Reading (no transaction) or Writing (with transaction)
    /// </summary>
    public enum SyncMode
    {
        Reading,
        Writing,
    }
}
