namespace Dotmim.Sync.Enumerations
{

    /// <summary>
    /// Determines what kind of action should be taken when an error is raised from the datasource
    /// during an insert / update or delete command.
    /// </summary>
    public enum ErrorResolution
    {
        /// <summary>
        /// Throw the error. Default value.Transaction is rollbacked.
        /// </summary>
        Throw,

        /// <summary>
        /// Ignore the error and continue to sync. Error will be stored
        /// locally in a separate batch info file.
        /// <para>
        /// Row is stored locally with a state of <see cref="SyncRowState.ApplyDeletedFailed"/>
        /// or <see cref="SyncRowState.ApplyModifiedFailed"/> depending on the row state.
        /// </para>
        /// </summary>
        ContinueOnError,

        /// <summary>
        /// Will try one more time once after all the others rows in the table.
        /// <para>
        /// If the error is raised again, an exception is thrown and transaction is rollback.
        /// </para>
        /// </summary>
        RetryOneMoreTimeAndThrowOnError,

        /// <summary>
        /// Will try one more time once after all the others rows in the table.
        /// <para>
        /// If the error is raised again, Sync continues normally and error will be stored locally in a
        /// separate batch info file with a state of <see cref="SyncRowState.ApplyDeletedFailed"/>
        /// or <see cref="SyncRowState.ApplyModifiedFailed"/> depending on the row state.
        /// </para>
        /// </summary>
        RetryOneMoreTimeAndContinueOnError,

        /// <summary>
        /// Row is stored locally and will be applied again on next sync. Sync continues normally and
        /// row is stored locally with a state of <see cref="SyncRowState.RetryDeletedOnNextSync"/>
        /// or <see cref="SyncRowState.RetryModifiedOnNextSync"/> depending on the row state.
        /// </summary>
        RetryOnNextSync,

        /// <summary>
        /// Considers the row as applied.
        /// </summary>
        Resolved,
    }

    /// <summary>
    /// Error action.
    /// </summary>
    public enum ErrorAction
    {
        /// <summary>
        /// An error occured and is still throw.
        /// Failed rows is incremented.
        /// </summary>
        Throw,

        /// <summary>
        /// An error occured and is log, but process will continue.
        /// Failed rows is incremented.
        /// </summary>
        Log,

        /// <summary>
        /// An error occurd, but process will continue.
        /// Neither failed rows nor applied rows are incremented.
        /// </summary>
        Ignore,

        /// <summary>
        /// An error occurd, but is marked as resolved and process will continue.
        /// Applied Rows is incremented.
        /// </summary>
        Resolved,
    }
}