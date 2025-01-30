namespace Dotmim.Sync.Enumerations
{
    /// <summary>
    /// Resolution when a conflict occurs.
    /// </summary>
    public enum ConflictResolution
    {
        /// <summary>
        /// Indicates that the change on the server is the conflict winner.
        /// </summary>
        ServerWins,

        /// <summary>
        /// Indicates that the change sent by the client is the conflict winner.
        /// </summary>
        ClientWins,

        /// <summary>
        /// Indicates that you will manage the conflict by filling the final row and sent it to both client and server.
        /// </summary>
        MergeRow,

        /// <summary>
        /// Indicates that you want to design this conflict as an error (and handle it from the OnApplyChangesErrorOccured interceptor).
        /// </summary>
        Throw,
    }
}