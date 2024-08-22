using System;

namespace Dotmim.Sync.DatabaseStringParsers
{
    /// <summary>
    /// Parse any object related to a schema. Can be a column, atable, a stored proc, a trigger etc ...
    /// </summary>
    public readonly ref struct ObjectParser
    {

        private readonly ReadOnlySpan<char> ownerName;
        private readonly ReadOnlySpan<char> objectName;

        /// <summary>
        /// Gets the first left quote found.
        /// </summary>
        public readonly char FirstLeftQuote { get; }

        /// <summary>
        /// Gets the first right quote found.
        /// </summary>
        public readonly char FirstRightQuote { get; }

        /// <inheritdoc cref="ObjectParser"/>
        public ObjectParser(ReadOnlySpan<char> input, char[] leftQuotes, char[] rightQuotes)
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
                    this.objectName = current.ToArray();
                }
                else if (tokensFoundCount == 2)
                {
                    this.ownerName = this.objectName;
                    this.objectName = current.ToArray();
                }
            }

            this.FirstLeftQuote = reader.FirstLeftQuote;
            this.FirstRightQuote = reader.FirstRightQuote;
        }

        /// <inheritdoc cref="ObjectParser"/>
        public ObjectParser(string input, char leftQuote, char rightQuote)
            : this(input.AsSpan(), [leftQuote], [rightQuote]) { }

        /// <inheritdoc cref="ObjectParser"/>
        public ObjectParser(string input)
            : this(input.AsSpan(), ['[', '`', '"'], [']', '`', '"']) { }

        /// <summary>
        /// Gets the name of the object (Column, Table, Stored proc, Trigger, etc ...)
        /// </summary>
        public string ObjectName => this.objectName.ToString();

        /// <summary>
        /// Gets the name of the owner (schema name if object is a stored proc, a trigger, a table. table name if object is a column).
        /// </summary>
        public string OwnerName => this.ownerName.ToString();

        /// <summary>
        /// Gets the quoted short name of the column, without table name.
        /// </summary>
        public string QuotedShortName => $"{this.FirstLeftQuote}{this.objectName.ToString()}{this.FirstRightQuote}";

        /// <summary>
        /// Gets the normalized short name of the table, without schema name or database name.
        /// </summary>
        public string NormalizedShortName
        {
            get
            {
                string tableNameString;
#if NET6_0_OR_GREATER
                if (this.objectName.Contains(' '))
#else
                if (this.objectName.Contains(" ".AsSpan(), SyncGlobalization.DataSourceStringComparison))
#endif
                {
#if NET8_0_OR_GREATER
                    var tableNameReplaced = TableParser.Replace(this.objectName, ' ', '_');
                    tableNameString = tableNameReplaced.ToString();
#else
                    tableNameString = this.objectName.ToString().Replace(' ', '_');
#endif
                }
                else
                {
                    tableNameString = this.objectName.ToString();
                }

                return tableNameString;
            }
        }
    }
}