using System;

namespace Dotmim.Sync.DatabaseStringParsers
{
    /// <summary>
    /// A low-level tokenizer that walks a span of characters and
    /// extracts quoted or unquoted “objects” (identifiers), optionally
    /// skipping dots (.) as separators.
    /// </summary>
    public ref struct ObjectsReader
    {
        private readonly ReadOnlySpan<char> input;
        private readonly char[] leftQuotes;
        private readonly char[] rightQuotes;
        private int position;

        /// <summary>
        /// After a successful <see cref="Read"/>, the span of characters
        /// that was just extracted (without its surrounding quotes).
        /// </summary>
        public ReadOnlySpan<char> Current { get; private set; }

        /// <summary>
        /// The very first left-quote character actually encountered
        /// when tokenizing. If no real quotes were found, this will
        /// default to the first element of <c>leftQuotes</c>.
        /// </summary>
        public char FirstLeftQuote { get; private set; }

        /// <summary>
        /// The very first right-quote character actually encountered
        /// when tokenizing. If no real quotes were found, this will
        /// default to the first element of <c>rightQuotes</c>.
        /// </summary>
        public char FirstRightQuote { get; private set; }

        /// <summary>
        /// Constructs a new reader over <paramref name="input"/>,
        /// recognizing any character in <paramref name="leftQuotes"/>
        /// as an opening quote and any in <paramref name="rightQuotes"/>
        /// as a closing quote.
        /// </summary>
        public ObjectsReader(ReadOnlySpan<char> input, char[] leftQuotes, char[] rightQuotes)
        {
            this.input = input;
            this.leftQuotes = leftQuotes;
            this.rightQuotes = rightQuotes;
            this.position = 0;
            this.Current = default;
            this.FirstLeftQuote = '\0';
            this.FirstRightQuote = '\0';
        }

        /// <summary>
        /// Advances to the next token. Returns <c>true</c> if a token
        /// was found; <c>false</c> when the end of the input is reached.
        /// </summary>
        public bool Read()
        {
            while (position < input.Length)
            {
                // Skip dots and whitespace
                while (position < input.Length && (input[position] == '.' || char.IsWhiteSpace(input[position])))
                    position++;

                if (position >= input.Length)
                    break;

                var c = input[position];

                // Quoted token?
                if (Array.IndexOf(leftQuotes, c) >= 0)
                {
                    char left = c;

                    // Find the next *any* right-quote
                    int start = position + 1;
                    int len = 0;
                    for (; start + len < input.Length; len++)
                    {
                        if (Array.IndexOf(rightQuotes, input[start + len]) >= 0)
                            break;
                    }

                    // If we never found a real closing quote, skip this '[' or '`'
                    if (start + len >= input.Length)
                    {
                        position++;
                        continue;
                    }

                    char right = input[start + len];
                    Current = input.Slice(start, len);
                    position = start + len + 1;

                    if (FirstLeftQuote == '\0') FirstLeftQuote = left;
                    if (FirstRightQuote == '\0') FirstRightQuote = right;

                    return true;
                }
                else
                {
                    // Unquoted token: read until next dot or end
                    int start = position;
                    int end = start;
                    while (end < input.Length && input[end] != '.')
                        end++;

                    var raw = input.Slice(start, end - start);

                    // Trim any trailing right-quote (e.g. stray backtick)
                    raw = raw.TrimEnd(rightQuotes);

                    Current = raw;
                    position = end;

                    // On the very first unquoted token, if we never saw a real quote,
                    // fall back to the *first* quote character of each array:
                    if (FirstLeftQuote == '\0') FirstLeftQuote = leftQuotes[0];
                    if (FirstRightQuote == '\0') FirstRightQuote = rightQuotes[0];

                    return true;
                }
            }

            return false;
        }
    }
}