namespace Dotmim.Sync.Web.Client
{
    /// <summary>
    /// Http steps involved during a sync beetween a proxy client and proxy server.
    /// </summary>
    public enum HttpStep
    {
        /// <summary>
        /// No step.
        /// </summary>
        None,

        /// <summary>
        /// Ensure schema.
        /// </summary>
        EnsureSchema,

        /// <summary>
        /// Ensure scopes.
        /// </summary>
        EnsureScopes,

        /// <summary>
        /// Send changes.
        /// </summary>
        SendChanges,

        /// <summary>
        /// Send changes in progress.
        /// </summary>
        SendChangesInProgress,

        /// <summary>
        /// Get changes.
        /// </summary>
        GetChanges,

        /// <summary>
        /// Get estimated changes count.
        /// </summary>
        GetEstimatedChangesCount,

        /// <summary>
        /// Get more changes.
        /// </summary>
        GetMoreChanges,

        /// <summary>
        /// Get changes in progress.
        /// </summary>
        GetChangesInProgress,

        /// <summary>
        /// Get snapshot.
        /// </summary>
        GetSnapshot,

        /// <summary>
        /// Get summary.
        /// </summary>
        GetSummary,

        /// <summary>
        /// Send end download changes.
        /// </summary>
        SendEndDownloadChanges,

        /// <summary>
        /// Get remote client timestamp.
        /// </summary>
        GetRemoteClientTimestamp,

        /// <summary>
        /// Get operation.
        /// </summary>
        GetOperation,

        /// <summary>
        /// End session.
        /// </summary>
        EndSession,
    }
}