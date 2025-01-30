namespace Dotmim.Sync.Enumerations
{

    /// <summary>
    /// Basic mode : Reading (no transaction) or Writing (with transaction).
    /// </summary>
    public enum SyncMode
    {
        /// <summary>
        /// No transaction mode.
        /// </summary>
        NoTransaction,

        /// <summary>
        /// With transaction mode.
        /// </summary>
        WithTransaction,
    }
}