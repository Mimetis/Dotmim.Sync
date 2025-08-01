using System;
using System.Collections.Generic;

namespace Dotmim.Sync.DatabaseStringParsers
{

    /// <summary>
    /// Parses a table name with optional schema, handling the same quoting rules.
    /// </summary>
    public readonly ref struct TableParser
    {

        private readonly ReadOnlySpan<char> schemaName;
        private readonly ReadOnlySpan<char> tableName;

        /// <summary>
        /// First left-quote encountered (or default fallback).
        /// </summary>
        public readonly char FirstLeftQuote { get; }

        /// <summary>
        /// First right-quote encountered (or default fallback).
        /// </summary>
        public readonly char FirstRightQuote { get; }

        /// <summary>
        /// Constructs a table parser over a raw span and quote arrays.
        /// </summary>
        public TableParser(ReadOnlySpan<char> input, char[] leftQuotes, char[] rightQuotes)
        {
            var reader = new ObjectsReader(input, leftQuotes, rightQuotes);

            // With an array to store the tokens, since List<T> cannot be used with ref structs like ReadOnlySpan<char>
            ReadOnlySpan<char> token1 = default, token2 = default;
            int tokenCount = 0;

            while (reader.Read())
            {
                if (tokenCount == 0)
                    token1 = reader.Current;
                else if (tokenCount == 1)
                    token2 = reader.Current;
                tokenCount++;
            }

            ReadOnlySpan<char> schema = default, table = default;

            if (tokenCount >= 2)
            {
                schema = token1;
                table = token2;
            }
            else if (tokenCount == 1)
            {
                // single token—maybe “schema.table” inside one quoted block?
                var tok = token1.ToString();
                var dot = tok.IndexOf('.');
                if (dot > 0 && dot < tok.Length - 1)
                {
                    schema = tok.AsSpan(0, dot);
                    table = tok.AsSpan(dot + 1);
                }
                else
                {
                    table = token1;
                }
            }

            schemaName = schema.Trim();   // drop stray whitespace
            tableName = table.Trim();

            this.FirstLeftQuote = reader.FirstLeftQuote == char.MinValue ? leftQuotes[0] : reader.FirstLeftQuote;
            this.FirstRightQuote = reader.FirstRightQuote == char.MinValue ? rightQuotes[0] : reader.FirstRightQuote;
        }

        /// <summary>
        /// Shortcut ctor for a single quote style (e.g. '[',']').
        /// </summary>
        public TableParser(string input, char leftQuote, char rightQuote)
            : this(input.AsSpan(), new[] { leftQuote }, new[] { rightQuote }) { }

        /// <summary>
        /// Default ctor: recognizes [ ] , ` ` and " " as quote styles.
        /// </summary>
        public TableParser(string input)
            : this(input.AsSpan(),
                   ObjectParser.openingQuotes,
                   ObjectParser.closingQuotes)
        { }

        /// <summary>
        /// The unquoted table name (e.g. “Customer”).
        /// </summary>
        public string TableName => tableName.ToString();

        /// <summary>
        /// The unquoted schema name (or empty).
        /// </summary>
        public string SchemaName => schemaName.ToString();

        /// <summary>
        /// Short name re-quoted with the first quote style (e.g. “[Customer]”).
        /// </summary>
        public string QuotedShortName =>
            $"{FirstLeftQuote}{tableName.ToString()}{FirstRightQuote}";

        /// <summary>
        /// Full name (schema + dot + table), re-quoted in that same style.
        /// </summary>
        public string QuotedFullName
        {
            get
            {
                if (string.IsNullOrEmpty(SchemaName))
                    return QuotedShortName;

                return $"{FirstLeftQuote}{schemaName.ToString()}{FirstRightQuote}"
                     + "."
                     + $"{FirstLeftQuote}{tableName.ToString()}{FirstRightQuote}";
            }
        }

        /// <summary>
        /// Normalized short name (underscores, no special chars).
        /// </summary>
        public string NormalizedShortName =>
            ObjectParser.ReplaceSpecialCharacters(tableName.ToString());

        /// <summary>
        /// Normalized full name (schema and table joined with “_”).
        /// </summary>
        public string NormalizedFullName
        {
            get
            {
                if (string.IsNullOrEmpty(SchemaName))
                    return NormalizedShortName;

                return ObjectParser.ReplaceSpecialCharacters(schemaName.ToString())
                     + "_"
                     + NormalizedShortName;
            }
        }
    }
}