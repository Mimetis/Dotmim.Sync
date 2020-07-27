using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Enumerations
{
    /// <summary>
    /// Defines the state that a synchronization session is in.
    /// </summary>
    public enum SyncSessionState
    {
        /// <summary>
        /// The session is ready to synchronize changes.
        /// </summary>
        Ready,

        /// <summary>
        /// The session is currently synchronizing changes.
        /// </summary>
        Synchronizing
    }
}
