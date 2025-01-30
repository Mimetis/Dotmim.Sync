using System;

namespace Dotmim.Sync.DatabaseStringParsers
{
    /// <summary>
    /// Parse a table and get the database, schema and table name and can be recomposed with quotes.
    /// </summary>
    public readonly ref struct TableParser
    {
        private readonly ReadOnlySpan<char> databaseName;
        private readonly ReadOnlySpan<char> schemaName;
        private readonly ReadOnlySpan<char> tableName;

        /// <summary>
        /// Gets the first left quote found.
        /// </summary>
        public readonly char FirstLeftQuote { get; }

        /// <summary>
        /// Gets the first right quote found.
        /// </summary>
        public readonly char FirstRightQuote { get; }

        /// <inheritdoc cref="TableParser"/>
        public TableParser(ReadOnlySpan<char> input, char[] leftQuotes, char[] rightQuotes)
        {
            var reader = new ObjectsReader(input, leftQuotes, rightQuotes);

            var tokensFoundCount = 0;

            while (reader.Read())
            {
                var current = reader.Current;

                tokensFoundCount++;

                // add the object to the list
                if (tokensFoundCount == 1)
                {
                    this.tableName = current.ToArray();
                }
                else if (tokensFoundCount == 2)
                {
                    this.schemaName = this.tableName;
                    this.tableName = current.ToArray();
                }
                else if (tokensFoundCount == 3)
                {
                    this.databaseName = this.schemaName;
                    this.schemaName = this.tableName;
                    this.tableName = current.ToArray();
                }
            }

            this.FirstLeftQuote = reader.FirstLeftQuote == char.MinValue ? leftQuotes[0] : reader.FirstLeftQuote;
            this.FirstRightQuote = reader.FirstRightQuote == char.MinValue ? rightQuotes[0] : reader.FirstRightQuote;
        }

        /// <inheritdoc cref="TableParser"/>
        public TableParser(string input, char leftQuote, char rightQuote)
            : this(input.AsSpan(), [leftQuote], [rightQuote]) { }

        /// <inheritdoc cref="TableParser"/>
        public TableParser(string input)
            : this(input.AsSpan(), ['[', '`', '"'], [']', '`', '"']) { }

        /// <summary>
        /// Gets the name of the table.
        /// </summary>
        public string TableName => this.tableName.ToString();

        /// <summary>
        /// Gets the name of the schema.
        /// </summary>
        public string SchemaName => this.schemaName.ToString();

        /// <summary>
        /// Gets the name of the database.
        /// </summary>
        public string DatabaseName => this.databaseName.ToString();

        /// <summary>
        /// Gets the quoted short name of the table, without schema name or database name.
        /// </summary>
        public string QuotedShortName => $"{this.FirstLeftQuote}{this.tableName.ToString()}{this.FirstRightQuote}";

        /// <summary>
        /// Gets the quoted full name of the table, including schema name and database name if any.
        /// </summary>
        public string QuotedFullName =>
            this.databaseName.Length == 0 && this.schemaName.Length == 0
                    ? $"{this.FirstLeftQuote}{this.tableName.ToString()}{this.FirstRightQuote}"
                    : this.databaseName.Length == 0
                    ? $"{this.FirstLeftQuote}{this.schemaName.ToString()}{this.FirstRightQuote}.{this.FirstLeftQuote}{this.tableName.ToString()}{this.FirstRightQuote}"
                    : $"{this.FirstLeftQuote}{this.databaseName.ToString()}{this.FirstRightQuote}.{this.FirstLeftQuote}{this.schemaName.ToString()}{this.FirstRightQuote}.{this.FirstLeftQuote}{this.tableName.ToString()}{this.FirstRightQuote}";

        /// <summary>
        /// Replace all occurrences of a character by another character from a ReadOnlySpan and returns a new Span.
        /// </summary>
        internal static Span<char> Replace(ReadOnlySpan<char> chars, char oldValue, char newValue)
        {
            var pool = new Span<char>(new char[chars.Length]);
#if NET8_0_OR_GREATER
            chars.Replace(pool, oldValue, newValue);
#else
            chars.CopyTo(pool);
            int pos;
            int slice = 0;
            while ((pos = chars.IndexOf(oldValue)) >= 0)
            {
                pool[pos + slice] = newValue;
                chars = chars.Slice(pos + 1);
                slice += pos + 1;
            }
#endif
            return pool;
        }

        /// <summary>
        /// Gets the normalized short name of the table, without schema name or database name.
        /// </summary>
        public string NormalizedShortName
        {
            get
            {
                string tableNameString;
#if NET6_0_OR_GREATER
                if (this.tableName.Contains(' '))
#else
                if (this.tableName.Contains(" ".AsSpan(), SyncGlobalization.DataSourceStringComparison))
#endif
                {
#if NET8_0_OR_GREATER
                    var tableNameReplaced = TableParser.Replace(this.tableName, ' ', '_');
                    tableNameString = tableNameReplaced.ToString();
#else
                    tableNameString = this.tableName.ToString().Replace(' ', '_');
#endif
                }
                else
                {
                    tableNameString = this.tableName.ToString();
                }

                return tableNameString;
            }
        }

        /// <summary>
        /// Gets the normalized full name of the table, including schema name and database name if any.
        /// </summary>
        public string NormalizedFullName
        {
            get
            {
                string tableNameString;
#if NET6_0_OR_GREATER
                if (this.tableName.Contains(' '))
#else
                if (this.tableName.Contains(" ".AsSpan(), SyncGlobalization.DataSourceStringComparison))
#endif
                {
#if NET8_0_OR_GREATER
                    var tableNameReplaced = TableParser.Replace(this.tableName, ' ', '_');
                    tableNameString = tableNameReplaced.ToString();
#else
                    tableNameString = this.tableName.ToString().Replace(' ', '_');
#endif
                }
                else
                {
                    tableNameString = this.tableName.ToString();
                }

                return this.databaseName.Length == 0 && this.schemaName.Length == 0
                    ? tableNameString
                    : this.databaseName.Length == 0
                    ? $"{this.schemaName.ToString()}_{tableNameString}"
                    : $"{this.databaseName.ToString()}_{this.schemaName.ToString()}_{tableNameString}";
            }
        }
    }
}