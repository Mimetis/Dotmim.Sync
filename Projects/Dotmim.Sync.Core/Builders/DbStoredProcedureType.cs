namespace Dotmim.Sync.Builders
{

    /// <summary>
    /// StoredProcedure type enumeration.
    /// </summary>
    public enum DbStoredProcedureType
    {
        /// <summary>
        /// Select changes stored procedure.
        /// </summary>
        SelectChanges,

        /// <summary>
        /// Select changes stored procedure with filters.
        /// </summary>
        SelectChangesWithFilters,

        /// <summary>
        /// Select initialized changes stored procedure.
        /// </summary>
        SelectInitializedChanges,

        /// <summary>
        /// Select initialized changes stored procedure with filters.
        /// </summary>
        SelectInitializedChangesWithFilters,

        /// <summary>
        /// Select a particular record stored procedure.
        /// </summary>
        SelectRow,

        /// <summary>
        /// Update a particular record stored procedure.
        /// </summary>
        UpdateRow,

        /// <summary>
        /// Delete a particular record stored procedure.
        /// </summary>
        DeleteRow,

        /// <summary>
        /// Bulk update rows stored procedure.
        /// </summary>
        BulkUpdateRows,

        /// <summary>
        /// Bulk delete rows stored procedure.
        /// </summary>
        BulkDeleteRows,

        /// <summary>
        /// Reset stored procedure.
        /// </summary>
        Reset,

        /// <summary>
        /// Bulk table type creation stored procedure.
        /// </summary>
        BulkTableType,
    }
}