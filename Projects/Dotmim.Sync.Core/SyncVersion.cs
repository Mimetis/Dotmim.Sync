using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync
{
    public static class SyncVersion
    {
        public static Version Current { get; } = new Version(0, 9, 8);

        public static Version EnsureVersion(string v) => v == "1" ? new Version(0, 6, 0) : new Version(v);
    }

}
