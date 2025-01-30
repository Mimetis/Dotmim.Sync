﻿namespace Dotmim.Sync.Enumerations
{
    /// <summary>
    /// Transaction mode during the sync process.
    /// </summary>
    public enum TransactionMode
    {
        /// <summary>
        /// Default mode for transaction, when applying changes.
        /// </summary>
        AllOrNothing,

        /// <summary>
        /// Each batch file will have its own transaction.
        /// </summary>
        PerBatch,

        /// <summary>
        /// No transaction during applying changes. very risky.
        /// </summary>
        None,
    }
}