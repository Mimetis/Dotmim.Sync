namespace Dotmim.Sync
{
    /// <summary>
    /// Represents a synchronization command that is prepared (or not).
    /// </summary>
    public class SyncPreparedCommand
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SyncPreparedCommand"/> class, using a command code name. By default, the command is not prepared.
        /// </summary>
        public SyncPreparedCommand(string commandCodeName)
        {
            this.CommandCodeName = commandCodeName;
            this.IsPrepared = false;
        }

        /// <summary>
        /// Gets or sets a value indicating whether gets or Sets if the command is prepared.
        /// </summary>
        public bool IsPrepared { get; set; }

        /// <summary>
        /// Gets the command code name.
        /// </summary>
        public string CommandCodeName { get; }
    }
}