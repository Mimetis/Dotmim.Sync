﻿using System;

namespace Dotmim.Sync.DatabaseStringParsers
{
    /// <summary>
    /// Parse a table and get the database, schema and table name and can be recomposed with quotes.
    /// </summary>
    public readonly ref struct TableParser
    {
        private readonly char leftQuote;
        private readonly char rightQuote;

        private readonly ReadOnlySpan<char> databaseName;
        private readonly ReadOnlySpan<char> schemaName;
        private readonly ReadOnlySpan<char> tableName;

        /// <inheritdoc cref="TableParser"/>
        public TableParser(ReadOnlySpan<char> input, char leftQuote, char rightQuote)
        {
            this.leftQuote = leftQuote;
            this.rightQuote = rightQuote;

            var reader = new ObjectsReader(input, leftQuote, rightQuote);

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
        }

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
        public string QuotedShortName => $"{this.leftQuote}{this.tableName.ToString()}{this.rightQuote}";

        /// <summary>
        /// Gets the quoted full name of the table, including schema name and database name if any.
        /// </summary>
        public string QuotedFullName =>
            this.databaseName.Length == 0 && this.schemaName.Length == 0
                    ? $"{this.leftQuote}{this.tableName.ToString()}{this.rightQuote}"
                    : this.databaseName.Length == 0
                    ? $"{this.leftQuote}{this.schemaName.ToString()}{this.rightQuote}.{this.leftQuote}{this.tableName.ToString()}{this.rightQuote}"
                    : $"{this.leftQuote}{this.databaseName.ToString()}{this.rightQuote}.{this.leftQuote}{this.schemaName.ToString()}{this.rightQuote}.{this.leftQuote}{this.tableName.ToString()}{this.rightQuote}";

        /// <summary>
        /// Replace all occurrences of a character by another character from a ReadOnlySpan and returns a new Span.
        /// </summary>
        internal static Span<char> Replace(ReadOnlySpan<char> chars, char oldValue, char newValue)
        {
            var pool = new Span<char>(new char[chars.Length]);

#if NET8_0_OR_GREATER
            chars.Replace(pool, oldValue, newValue);
#else
            int pos;
            while ((pos = chars.IndexOf(oldValue)) >= 0)
            {
                pool[pos] = newValue;
                chars = chars.Slice(pos + 1);
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
                    var tableNameReplaced = Replace(this.tableName, ' ', '_');
                    tableNameString = tableNameReplaced.ToString();
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
                    var tableNameReplaced = Replace(this.tableName, ' ', '_');
                    tableNameString = tableNameReplaced.ToString();
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