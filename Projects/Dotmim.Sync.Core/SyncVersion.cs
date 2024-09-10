using System;
using System.Reflection;

namespace Dotmim.Sync
{
    /// <summary>
    /// Contains the current version of the library.
    /// </summary>
    public static class SyncVersion
    {
        private static readonly Lazy<Version> lazyVersion = new Lazy<Version>(() =>
        {
            return new Version(typeof(CoreProvider).GetTypeInfo()
                .Assembly.GetCustomAttribute<AssemblyFileVersionAttribute>().Version);
        });

        /// <summary>
        /// Gets the current version of the library.
        /// </summary>
        public static Version Current => lazyVersion.Value;

        /// <summary>
        /// Ensure the version is correct.
        /// </summary>
        public static Version EnsureVersion(string v) => v == "1" ? new Version(0, 6, 0) : new Version(v);
    }
}