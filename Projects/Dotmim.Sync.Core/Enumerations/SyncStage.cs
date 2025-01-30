namespace Dotmim.Sync.Enumerations
{
    /// <summary>
    /// Sync progress step. Used for the user feedback.
    /// </summary>
    public enum SyncStage
    {
        /// <summary>
        /// Sync is not in a specific stage.
        /// </summary>
        None = 0,

        /// <summary>
        /// Begin a new sync session.
        /// </summary>
        BeginSession,

        /// <summary>
        /// End a sync session.
        /// </summary>
        EndSession,

        /// <summary>
        /// Scope loading stage.
        /// </summary>
        ScopeLoading,

        /// <summary>
        /// Scope writing stage.
        /// </summary>
        ScopeWriting,

        /// <summary>
        /// Creating a snapshot stage.
        /// </summary>
        SnapshotCreating,

        /// <summary>
        /// Applying a snapshot stage.
        /// </summary>
        SnapshotApplying,

        /// <summary>
        /// Schema provisioning stage.
        /// </summary>
        Provisioning,

        /// <summary>
        /// Schema deprovisioning stage.
        /// </summary>
        Deprovisioning,

        /// <summary>
        /// Selecting changes stage.
        /// </summary>
        ChangesSelecting,

        /// <summary>
        /// Applying changes stage.
        /// </summary>
        ChangesApplying,

        /// <summary>
        /// Migration stage.
        /// </summary>
        Migrating,

        /// <summary>
        /// Cleaning metadata stage.
        /// </summary>
        MetadataCleaning,
    }
}