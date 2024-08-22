using System;

namespace Dotmim.Sync.Enumerations
{
    /// <summary>
    /// Current sync operation.
    /// </summary>
    [Flags]
    public enum SyncOperation
    {
        /// <summary>
        /// Normal synchronization.
        /// </summary>
#pragma warning disable CA1008 // Enums should have zero value
        Normal = 0,
#pragma warning restore CA1008 // Enums should have zero value

        /// <summary>
        /// Reinitialize the whole sync database, applying all rows from the server to the client.
        /// </summary>
        Reinitialize = 1,

        /// <summary>
        /// Reinitialize the whole sync database, applying all rows from the server to the client, after trying a client upload.
        /// </summary>
        ReinitializeWithUpload = 2,

        /// <summary>
        /// Drop all the sync metadatas even tracking tables and scope infos and make a full sync again.
        /// </summary>
        DropAllAndSync = 4,

        /// <summary>
        /// Drop all the sync metadatas even tracking tables and scope infos and exit.
        /// </summary>
        DropAllAndExit = 8,

        /// <summary>
        /// Deprovision stored procedures and triggers and sync again.
        /// </summary>
        DeprovisionAndSync = 16,

        /// <summary>
        /// Exit a Sync session without syncing.
        /// </summary>
        AbortSync = 32,
    }
}