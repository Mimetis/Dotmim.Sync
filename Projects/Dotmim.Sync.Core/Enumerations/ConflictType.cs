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
        /// The remote datasource raised an unique key constraint error.
        /// </summary>
        UniqueKeyConstraint,

        // -------------------------------
        // Classic conflicts on update / update or deleted / deleted.
        // -------------------------------

        /// <summary>
        /// The Remote and Local datasources have both updated the same row.
        /// </summary>
        RemoteExistsLocalExists,

        /// <summary>
        /// The Remote and Local datasource have both deleted the same row.
        /// </summary>
        RemoteIsDeletedLocalIsDeleted,

        // -------------------------------
        // Updated or Inserted on one side and Not Exists on the other
        // -------------------------------

        /// <summary>
        /// The Remote datasource has updated or inserted a row that does not exists in the local datasource.
        /// </summary>
        RemoteExistsLocalNotExists,

        /// <summary>
        /// The Local datasource has inserted or updated a row that does not exists in the Remote datasource.
        /// </summary>
        RemoteNotExistsLocalExists,

        // -------------------------------
        // Deleted on one side and Updated or Inserted on the other
        // -------------------------------

        /// <summary>
        /// The Remote datasource has inserted or updated a row that the Local datasource has deleted.
        /// </summary>
        RemoteExistsLocalIsDeleted,

        /// <summary>
        /// The Remote datasource has deleted a row that the Local datasource has inserted or updated.
        /// </summary>
        RemoteIsDeletedLocalExists,

        // -------------------------------
        // Deleted on one side and Not Exists on the other
        // -------------------------------

        // The Local datasource has deleted a row that does not exists in the Remote datasource
        // Note : this Case can't happen
        // From the server point of view : Remote Not Exists means client has not the row. SO it will just not send anything to the server
        // From the client point of view : Remote Not Exists means server has not the row. SO it will just not send back anything to client
        // RemoteNotExistsLocalIsDeleted,

        /// <summary>
        /// The Remote datasource has deleted a row that does not exists in the Local datasource.
        /// </summary>
        RemoteIsDeletedLocalNotExists,
    }
}