namespace Dotmim.Sync.Builders
{

    /// <summary>
    /// Parser string options.
    /// </summary>
    public class ParserString
    {
        /// <summary>
        /// Gets or Sets the quote prefix.
        /// </summary>
        public string QuotePrefix { get; set; } = "[";

        /// <summary>
        /// Gets or Sets the quote suffix.
        /// </summary>
        public string QuoteSuffix { get; set; } = "]";

        /// <summary>
        /// Gets or Sets the normalized schema name.
        /// </summary>
        public string SchemaName { get; set; }

        /// <summary>
        /// Gets or Sets the normalized object name (usually the table name).
        /// </summary>
        public string ObjectName { get; set; }

        /// <summary>
        /// Gets or Sets the normalized database name.
        /// </summary>
        public string DatabaseName { get; set; }

        /// <summary>
        /// Gets or Sets the quoted schema name.
        /// </summary>
        public string QuotedSchemaName { get; set; }

        /// <summary>
        /// Gets or Sets the quoted object name (usually the quoted table name).
        /// </summary>
        public string QuotedObjectName { get; set; }

        /// <summary>
        /// Gets or Sets the quoted database name.
        /// </summary>
        public string QuotedDatabaseName { get; set; }
    }
}