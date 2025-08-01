using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace Dotmim.Sync.DatabaseStringParsers
{

    /// <summary>
    /// Parses a “schema‐qualified” object identifier (column, proc, etc.).
    /// </summary>
    public readonly ref struct ObjectParser
    {
        internal static readonly char[] openingQuotes = new[] { '[', '`', '"' };
        internal static readonly char[] closingQuotes = new[] { ']', '`', '"' };

        private readonly ReadOnlySpan<char> ownerName;
        private readonly ReadOnlySpan<char> objectName;

        /// <summary>
        /// The first left-quote character found in the input (or default).
        /// </summary>
        public readonly char FirstLeftQuote { get; }

        /// <summary>
        /// The first right-quote character found in the input (or default).
        /// </summary>
        public readonly char FirstRightQuote { get; }

        /// <summary>
        /// Constructs a parser over a raw span, using the given quote arrays.
        /// </summary>
        public ObjectParser(ReadOnlySpan<char> input, char[] leftQuotes, char[] rightQuotes)
        {
            var reader = new ObjectsReader(input, leftQuotes, rightQuotes);
            var tokensFoundCount = 0;
            ReadOnlySpan<char> owner = default;
            ReadOnlySpan<char> obj = default;

            while (reader.Read())
            {
                tokensFoundCount++;
                if (tokensFoundCount == 1)
                    obj = reader.Current;
                else if (tokensFoundCount == 2)
                {
                    owner = obj;
                    obj = reader.Current;
                }
            }

            this.ownerName = owner;
            this.objectName = obj;

            this.FirstLeftQuote = reader.FirstLeftQuote;
            this.FirstRightQuote = reader.FirstRightQuote;
        }

        /// <summary>
        /// Shortcut for parsing from a single‐quote‐pair (e.g. '[',']').
        /// </summary>
        public ObjectParser(string input, char leftQuote, char rightQuote)
            : this(input.AsSpan(), new[] { leftQuote }, new[] { rightQuote }) { }

        /// <summary>
        /// Default parser: recognizes [ ] , ` ` and " " as quote pairs.
        /// </summary>
        public ObjectParser(string input)
            : this(input.AsSpan(),
                   ObjectParser.openingQuotes,
                   ObjectParser.closingQuotes)
        { }

        /// <summary>
        /// The unquoted object name (e.g. “ID”).
        /// </summary>
        public string ObjectName => objectName.ToString();

        /// <summary>
        /// The owner/schema part (e.g. “Customer” in “Customer.ID”), or empty.
        /// </summary>
        public string OwnerName => ownerName.ToString();

        /// <summary>
        /// The short name re-quoted with the first quote style (e.g. "[ID]").
        /// </summary>
        public string QuotedShortName =>
            $"{FirstLeftQuote}{objectName.ToString()}{FirstRightQuote}";

        /// <summary>
        /// A safe, underscore-only version of the object name.
        /// </summary>
        public string NormalizedShortName =>
            ReplaceSpecialCharacters(objectName.ToString());

        /// <summary>
        /// Replaces spaces, punctuation, diacritics, etc. with underscores.
        /// </summary>
        public static string ReplaceSpecialCharacters(string input)
        {
            if (string.IsNullOrEmpty(input))
                return input;

            var buffer = new char[input.Length];
            int idx = 0;
            var normalized = input.Normalize(NormalizationForm.FormD);

            const string specialChars = "-./\\:;,|*?!\"'()[]{}&%#$@=+<>";
            foreach (var ch in normalized)
            {
                var cat = CharUnicodeInfo.GetUnicodeCategory(ch);
                if (cat == UnicodeCategory.NonSpacingMark)
                    continue;
#if NETSTANDARD2_0
                if (char.IsWhiteSpace(ch) || specialChars.IndexOf(ch) >= 0)
#elif NET6_0_OR_GREATER
                if (char.IsWhiteSpace(ch) || specialChars.Contains(ch))
#endif
                {
                    buffer[idx++] = '_';
                }
                else
                {
                    buffer[idx++] = ch;
                }
            }

            return new string(buffer, 0, idx);
        }
    }
}