using System;

namespace Dotmim.Sync.DatabaseStringParsers
{
    /// <summary>
    /// Parse a table and get the database, schema and table name and can be recomposed with quotes.
    /// </summary>
    public readonly ref struct ColumnParser
    {
        private readonly char leftQuote;
        private readonly char rightQuote;

        private readonly ReadOnlySpan<char> tableName;
        private readonly ReadOnlySpan<char> columnName;

        /// <inheritdoc cref="TableParser"/>
        public ColumnParser(ReadOnlySpan<char> input, char leftQuote, char rightQuote)
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
                    this.columnName = current.ToArray();
                }
                else if (tokensFoundCount == 2)
                {
                    this.tableName = this.columnName;
                    this.columnName = current.ToArray();
                }
            }
        }

        /// <summary>
        /// Gets the name of the column.
        /// </summary>
        public string ColumnName => this.columnName.ToString();

        /// <summary>
        /// Gets the name of the table.
        /// </summary>
        public string TableName => this.tableName.ToString();

        /// <summary>
        /// Gets the quoted short name of the column, without table name.
        /// </summary>
        public string QuotedShortName => $"{this.leftQuote}{this.columnName.ToString()}{this.rightQuote}";

        /// <summary>
        /// Gets the normalized short name of the table, without schema name or database name.
        /// </summary>
        public string NormalizedShortName
        {
            get
            {
                string tableNameString;
#if NET6_0_OR_GREATER
                if (this.columnName.Contains(' '))
#else
                if (this.columnName.Contains(" ".AsSpan(), SyncGlobalization.DataSourceStringComparison))
#endif
                {
                    var tableNameReplaced = TableParser.Replace(this.columnName, ' ', '_');
                    tableNameString = tableNameReplaced.ToString();
                }
                else
                {
                    tableNameString = this.columnName.ToString();
                }

                return tableNameString;
            }
        }
    }
}