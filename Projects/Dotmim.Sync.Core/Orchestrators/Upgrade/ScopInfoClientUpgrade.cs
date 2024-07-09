namespace Dotmim.Sync
{
    /// <summary>
    /// Scope info client upgrade.
    /// </summary>
    public class ScopeInfoClientUpgrade
    {
        /// <summary>
        /// Gets or sets the scope name.
        /// </summary>
        public string ScopeName { get; set; }

        /// <summary>
        /// Gets the parameters.
        /// </summary>
        public SyncParameters Parameters { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="ScopeInfoClientUpgrade"/> class.
        /// </summary>
        public ScopeInfoClientUpgrade() => this.Parameters = new SyncParameters();
    }
}