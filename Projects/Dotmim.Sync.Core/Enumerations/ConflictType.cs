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
        /// The Remote datasource update a row and the Local datasource insert a row with same key
        /// </summary>
        RemoteUpdateLocalInsert,

        /// <summary>
        /// The Remote and Local datasources both updated the same row.
        /// </summary>
        RemoteUpdateLocalUpdate,

        /// <summary>
        /// The Remote datasource updated a row that the Local datasource deleted.
        /// </summary>
        RemoteUpdateLocalDelete,

        /// <summary>
        /// The Remote datasource deleted a row that the Local datasource insert with the same key.
        /// </summary>
        RemoteDeleteLocalInsert,

        /// <summary>
        /// The Remote datasource deleted a row that the Local datasource updated.
        /// </summary>
        RemoteDeleteLocalUpdate,

        /// <summary>
        /// The Remote and Local datasource both deleted the same row.
        /// </summary>
        RemoteDeleteLocalDelete,

        /// <summary>
        /// The Remote and Local datasource both inserted a row that has the same primary key value. This caused a primary key violation.
        /// </summary>
        RemoteInsertLocalInsert,

        /// <summary>
        /// The Remote datasource insert a row that the Local datasource updated.
        /// </summary>
        RemoteInsertLocalUpdate,

        /// <summary>
        /// The Remote datasource insert a row that the Local datasource delete.
        /// </summary>
        RemoteInsertLocalDelete,

        /// <summary>
        /// The Remote peer deleted a row that the Local peer updated, and the metadata for that row was cleaned up.
        /// </summary>
        RemoteCleanedupDeleteLocalUpdate,

        /// <summary>
        /// The Remote peer update a row that the Local never had.
        /// </summary>
        RemoteUpdateLocalNoRow,

        /// <summary>
        /// The Remote peer insert a row that the Local never had.
        /// </summary>
        RemoteInsertLocalNoRow,

        /// <summary>
        /// The Remote peer delete a row that the Local never had.
        /// </summary>
        RemoteDeleteLocalNoRow
    }
}
