using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Enumerations
{
    public enum ConflictResolution
    {
        /// <summary>
        /// Indicates that the change on the server is the conflict winner
        /// </summary>
        ServerWins,

        /// <summary>
        /// Indicates that the change sent by the client is the conflict winner
        /// </summary>
        ClientWins,

        /// <summary>
        /// Indicates that you will manage the conflict by filling the final row and sent it to both client and server
        /// </summary>
        MergeRow,

        /// <summary>
        /// Indicates that you want to rollback the whole sync process
        /// </summary>
        Rollback,

        /// <summary>
        /// Ignore the row in conflict and try to continue sync
        /// </summary>
        Ignore

    }
}
