namespace Dotmim.Sync.Builders
{

    /// <summary>
    /// Trigger type enumeration (Insert, Update, Delete).
    /// </summary>
    public enum DbTriggerType
    {
        /// <summary>
        /// Insert trigger.
        /// </summary>
        Insert,

        /// <summary>
        /// Update trigger.
        /// </summary>
        Update,

        /// <summary>
        /// Delete trigger.
        /// </summary>
        Delete,
    }
}