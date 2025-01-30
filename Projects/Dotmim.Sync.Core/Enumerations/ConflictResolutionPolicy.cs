namespace Dotmim.Sync.Enumerations
{
    /// <summary>
    /// Represents the options for the conflict resolution policy to use for synchronization.
    /// Used in the configuration class.
    /// </summary>
    public enum ConflictResolutionPolicy
    {
        /// <summary>
        /// Indicates that the change on the server wins in case of a conflict.
        /// </summary>
        ServerWins,

        /// <summary>
        /// Indicates that the change sent by the client wins in case of a conflict.
        /// </summary>
        ClientWins,
    }
}