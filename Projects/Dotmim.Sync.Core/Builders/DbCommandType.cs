namespace Dotmim.Sync.Builders
{
    /// <summary>
    /// All commands type used during sync.
    /// </summary>
    public enum DbCommandType
    {
        /// <summary>
        /// No command type specified.
        /// </summary>
        None,

        /// <summary>
        /// Select changes command from the current provider.
        /// </summary>
        SelectChanges,

        /// <summary>
        /// Select initial changes command from the server provider to be applied on client side.
        /// </summary>
        SelectInitializedChanges,

        /// <summary>
        /// Select initial changes command from the server provider, with filters, to be applied on client side.
        /// </summary>
        SelectInitializedChangesWithFilters,

        /// <summary>
        /// Select changes command from the current provider, with filters.
        /// </summary>
        SelectChangesWithFilters,

        /// <summary>
        /// Select a particular record command.
        /// </summary>
        SelectRow,

        /// <summary>
        /// Update a particular record command.
        /// </summary>
        UpdateRow,

        /// <summary>
        /// Insert a particular record command.
        /// </summary>
        InsertRow,

        /// <summary>
        /// Delete a particular record command.
        /// </summary>
        DeleteRow,

        /// <summary>
        /// Delete constraints command.
        /// </summary>
        DisableConstraints,

        /// <summary>
        /// Enable constraints command.
        /// </summary>
        EnableConstraints,

        /// <summary>
        /// Delete metadata command.
        /// </summary>
        DeleteMetadata,

        /// <summary>
        /// Update metadata command.
        /// </summary>
        UpdateMetadata,

        /// <summary>
        /// Select metadata command.
        /// </summary>
        SelectMetadata,

        /// <summary>
        /// Insert trigger command.
        /// </summary>
        InsertTrigger,

        /// <summary>
        /// Update trigger command.
        /// </summary>
        UpdateTrigger,

        /// <summary>
        /// Delete trigger command.
        /// </summary>
        DeleteTrigger,

        /// <summary>
        /// Update rows command.
        /// </summary>
        UpdateRows,

        /// <summary>
        /// Insert rows command.
        /// </summary>
        InsertRows,

        /// <summary>
        /// delete rows command.
        /// </summary>
        DeleteRows,

        /// <summary>
        /// Bulk table type creation command.
        /// </summary>
        BulkTableType,

        /// <summary>
        /// Update untracked rows command on client side.
        /// </summary>
        UpdateUntrackedRows,

        /// <summary>
        /// Reset command.
        /// </summary>
        Reset,

        /// <summary>
        /// Pre command before calling UpdateRows command.
        /// </summary>
        PreUpdateRows,

        /// <summary>
        /// Pre command before calling InsertRows command.
        /// </summary>
        PreInsertRows,

        /// <summary>
        /// Pre command before calling DeleteRows command.
        /// </summary>
        PreDeleteRows,

        /// <summary>
        /// Pre command before calling UpdateRow command.
        /// </summary>
        PreUpdateRow,

        /// <summary>
        /// Pre command before calling InsertRow command.
        /// </summary>
        PreInsertRow,

        /// <summary>
        /// Pre command before calling DeleteRow command.
        /// </summary>
        PreDeleteRow,
    }
}