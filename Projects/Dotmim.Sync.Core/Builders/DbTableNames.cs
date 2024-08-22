namespace Dotmim.Sync.Builders
{

    /// <summary>
    /// Parsed definition of a table.
    /// </summary>
    public class DbTableNames
    {

        /// <inheritdoc cref="DbTableNames"/>
        public DbTableNames() { }

        /// <inheritdoc cref="DbTableNames"/>
        public DbTableNames(char quotePrefix, char quoteSuffix,
            string name, string normalizedFullName, string normalizedName,
            string quotedFullName, string quotedName, string schemaName)
        {
            this.QuotePrefix = quotePrefix;
            this.QuoteSuffix = quoteSuffix;
            this.Name = name;
            this.NormalizedFullName = normalizedFullName;
            this.NormalizedName = normalizedName;
            this.QuotedFullName = quotedFullName;
            this.QuotedName = quotedName;
            this.SchemaName = schemaName;
        }

        /// <summary>
        /// Gets or sets the prefix quote character.
        /// </summary>
        public char QuotePrefix { get; set; }

        /// <summary>
        /// Gets or sets the suffix quote character.
        /// </summary>
        public char QuoteSuffix { get; set; }

        /// <summary>
        /// Gets or sets the parsed table name, without any quotes characters.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the parsed normalized table full name (with schema, if any).
        /// </summary>
        public string NormalizedFullName { get; set; }

        /// <summary>
        /// Gets or sets the parsed normalized table short name (without schema, if any).
        /// </summary>
        public string NormalizedName { get; set; }

        /// <summary>
        /// Gets or sets the parsed quoted table full name (with schema, if any).
        /// </summary>
        public string QuotedFullName { get; set; }

        /// <summary>
        /// Gets the parsed quoted table short name (without schema, if any).
        /// </summary>
        public string QuotedName { get; }

        /// <summary>
        /// Gets or sets the parsed table schema name. if empty, "dbo" is returned.
        /// </summary>
        public string SchemaName { get; set; }
    }
}