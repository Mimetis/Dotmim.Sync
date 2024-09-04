using System;
using System.Reflection;

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
        public static Version Current
        {
            get
            {
                var assemblyVersion = new Version(typeof(CoreProvider).GetTypeInfo()
                    .Assembly.GetCustomAttribute<AssemblyFileVersionAttribute>().Version);

                return assemblyVersion;
            }
        }

        /// <summary>
        /// Ensure the version is correct.
        /// </summary>
        public static Version EnsureVersion(string v) => v == "1" ? new Version(0, 6, 0) : new Version(v);
    }
}