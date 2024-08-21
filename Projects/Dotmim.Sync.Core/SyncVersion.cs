using System;

namespace Dotmim.Sync
{
    /// <summary>
    /// Contains the current version of the library.
    /// </summary>
    public static class SyncVersion
    {
        /// <summary>
        /// Gets the current version of the library.
        /// </summary>
        public static Version Current { get; } = new Version(1, 1, 0);

        /// <summary>
        /// Ensure the version is correct.
        /// </summary>
        public static Version EnsureVersion(string v) => v == "1" ? new Version(0, 6, 0) : new Version(v);
    }
}