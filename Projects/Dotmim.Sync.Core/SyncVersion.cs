using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync
{
    public static class SyncVersion
    {
        public static Version Current { get; } = new Version(0, 8, 0);

        public static Version EnsureVersion(string v) => v == "1" ? new Version(0, 5, 7) : new Version(v);
    }

}
