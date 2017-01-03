using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Enumerations
{
    /// <summary>
    /// Defines the types of conflicts that can occur during synchronization.
    /// </summary>
    public enum ConflictType
    {
        /// <summary>
        /// The peer database threw an exception while applying a change.
        /// </summary>
        ErrorsOccurred,

        /// <summary>
        /// The local datasource update a row and the remote datasource insert a row with same key
        /// </summary>
        [Obsolete("this can't happened")]
        LocalUpdateRemoteInsert,

        /// <summary>
        /// The local and remote datasources both updated the same row.
        /// </summary>
        LocalUpdateRemoteUpdate,

        /// <summary>
        /// The local datasource updated a row that the remote datasource deleted.
        /// </summary>
        LocalUpdateRemoteDelete,

        /// <summary>
        /// The local datasource deleted a row that the remote datasource insert with the same key.
        /// </summary>
        [Obsolete("this can't happened")]
        LocalDeleteRemoteInsert,

        /// <summary>
        /// The local datasource deleted a row that the remote datasource updated.
        /// </summary>
        LocalDeleteRemoteUpdate,

        /// <summary>
        /// The local and remote datasource both deleted the same row.
        /// </summary>
        LocalDeleteRemoteDelete,

        /// <summary>
        /// The local and remote datasource both inserted a row that has the same primary key value. This caused a primary key violation.
        /// </summary>
        LocalInsertRemoteInsert,

        /// <summary>
        /// The local datasource insert a row that the remote datasource updated.
        /// </summary>
        [Obsolete("this can't happened")]
        LocalInsertRemoteUpdate,

        /// <summary>
        /// The local datasource insert a row that the remote datasource delete.
        /// </summary>
        LocalInsertRemoteDelete,

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
        LocalNoRowRemoteInsert,

        /// <summary>
        /// The local peer has no row that the remote peer delete.
        /// </summary>
        LocalNoRowRemoteDelete
    }
}
