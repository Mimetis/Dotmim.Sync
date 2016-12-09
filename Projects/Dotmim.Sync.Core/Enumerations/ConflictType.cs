using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Enumerations
{
    /// <summary>Defines the types of conflicts that can occur during synchronization.</summary>
    public enum ConflictType
    {
        /// <summary>
        /// The peer database threw an exception while applying a change.
        /// </summary>
        ErrorsOccurred,
        
        /// <summary>
        /// The local and remote peers both updated the same row.
        /// </summary>
        LocalUpdateRemoteUpdate,
        
        /// <summary>
        /// The local peer updated a row that the remote peer deleted.
        /// </summary>
        LocalUpdateRemoteDelete,
        
        /// <summary>
        /// The local peer deleted a row that the remote peer updated.
        /// </summary>
        LocalDeleteRemoteUpdate,
        
        /// <summary>
        /// The local and remote peers both inserted a row that has the same primary key value. This caused a primary key violation.
        /// </summary>
        LocalInsertRemoteInsert,
        
        /// <summary>
        /// The local and remote peers both deleted the same row.
        /// </summary>
        LocalDeleteRemoteDelete,
        
        /// <summary>
        /// The local peer deleted a row that the remote peer updated, and the metadata for that row was cleaned up.
        /// </summary>
        LocalCleanedupDeleteRemoteUpdate,

        /// <summary>
        /// The local peer has no row that the remote peer updated.
        /// </summary>
        LocalNoRowRemoteUpdate,

        /// <summary>
        /// The local peer has no row that the remote peer insert.
        /// </summary>
        LocalNoRowRemoteInsert
    }
}
