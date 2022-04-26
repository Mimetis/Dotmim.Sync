using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Enumerations
{

    /// <summary>
    /// Defines where occured the exception
    /// </summary>
    public enum SyncSide
    {
        /// <summary>
        /// Occurs when something comes from LocalOrchestrator
        /// </summary>
        ClientSide,

        /// <summary>
        /// Occurs when something comes from RemoteOrchestrator
        /// </summary>
        ServerSide
    }
}
