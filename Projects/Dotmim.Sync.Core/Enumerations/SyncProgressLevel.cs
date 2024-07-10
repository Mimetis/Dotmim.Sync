namespace Dotmim.Sync.Enumerations
{

    /// <summary>
    /// Defines IProgress levels of verbosity.
    /// </summary>
    public enum SyncProgressLevel
    {
        /// <summary>
        /// Progress that contain the most detailed messages and the Sql statement executed.
        /// </summary>
        Sql,

        /// <summary>
        /// Progress that contain the most detailed messages. These messages may contain sensitive application data.
        /// </summary>
        Trace,

        /// <summary>
        /// Progress that are used for interactive investigation during development.
        /// </summary>
        Debug,

        /// <summary>
        /// Progress that track the general flow of the application.
        /// </summary>
        Information,

        /// <summary>
        /// Specifies that a progress output should not write any messages.
        /// </summary>
        None,
    }
}