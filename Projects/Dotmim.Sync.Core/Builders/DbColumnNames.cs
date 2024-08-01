namespace Dotmim.Sync.Builders
{
    /// <summary>
    /// Parsed names of a column.
    /// </summary>
    public record DbColumnNames
    {

        /// <inheritdoc cref="DbColumnNames"/>
        public DbColumnNames() { }

        /// <inheritdoc cref="DbColumnNames"/>
        public DbColumnNames(string quotedName, string normalizedName)
        {
            this.NormalizedName = normalizedName;
            this.QuotedName = quotedName;
        }

        /// <summary>
        /// Gets the parsed quoted column short name (without schema, if any).
        /// </summary>
        public string QuotedName { get; }

        /// <summary>
        /// Gets the parsed normalized column short name (without schema, if any).
        /// </summary>
        public string NormalizedName { get; }
    }
}