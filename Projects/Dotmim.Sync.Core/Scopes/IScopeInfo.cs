using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync
{
    public interface IScopeInfo
    {
        string Name { get; set; }
        SyncSet Schema { get; set; }
        SyncSetup Setup { get; set; }
        string Version { get; set; }
    }
}
