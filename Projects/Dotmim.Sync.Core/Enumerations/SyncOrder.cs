using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Enumerations
{
    [Flags]
    public enum SyncOrder
    {
        /// <summary>
        /// Normal synchronization
        /// </summary>
        Normal = 0,

        /// <summary>
        /// Reinitialize the whole sync database, applying all rows from the server to the client
        /// </summary>
        Reinitialize = 1,
        
        /// <summary>
        /// Reinitialize the whole sync database, applying all rows from the server to the client, after trying a client upload
        /// </summary>
        ReinitializeWithUpload = 2,

        /// <summary>
        /// Drop all the sync metadatas even tracking tables and scope infos and make a full sync again
        /// </summary>
        DropAllAndSync = 4,

        /// <summary>
        /// Drop all the sync metadatas even tracking tables and scope infos and exit
        /// </summary>
        DropAllAndExit = 8,

        /// <summary>
        /// Deprovision stored procedures & triggers and sync again
        /// </summary>
        DeprovisionAndSync = 16,
    }
}
