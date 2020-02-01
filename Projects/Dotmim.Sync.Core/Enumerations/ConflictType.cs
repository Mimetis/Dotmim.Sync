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
        /// The remote datasource raised an unique key constraint error
        /// </summary>
        UniqueKeyConstraint,

        /// <summary>
        /// The Remote and Local datasources both updated the same row.
        /// </summary>
        RemoteExistsLocalExists,

        /// <summary>
        /// The Remote datasource updated a row that the Local datasource deleted.
        /// </summary>
        RemoteExistsLocalIsDeleted,

        /// <summary>
        /// The Remote datasource deleted a row that the Local datasource updated.
        /// </summary>
        RemoteIsDeletedLocalExists,

        /// <summary>
        /// The Remote and Local datasource both deleted the same row.
        /// </summary>
        RemoteIsDeletedLocalIsDeleted,
 
        /// <summary>
        /// The Remote peer deleted a row that the Local peer updated, and the metadata for that row was cleaned up.
        /// </summary>
        RemoteCleanedupDeleteLocalUpdate,

        /// <summary>
        /// The Remote peer update a row that the Local never had.
        /// </summary>
        RemoteExistsLocalNotExists,

        /// <summary>
        /// The Remote peer delete a row that the Local never had.
        /// </summary>
        RemoteIsDeletedLocalNotExists
    }
}
