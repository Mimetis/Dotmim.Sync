using System;
using System.Linq;

namespace Dotmim.Sync.DatabaseStringParsers
{
    /// <summary>
    /// Read database objects from a string.
    /// </summary>
    public ref struct ObjectsReader
    {
        private readonly ReadOnlySpan<char> input;
        private readonly char[] leftQuotes;
        private readonly char[] rightQuotes;
        private int dataPos = 0;

        /// <summary>
        /// Gets get current object.
        /// </summary>
        public ReadOnlySpan<char> Current { get; private set; }

        /// <summary>
        /// Gets the first left quote found.
        /// </summary>
        public char FirstLeftQuote { get; private set; }

        /// <summary>
        /// Gets the first right quote found.
        /// </summary>
        public char FirstRightQuote { get; private set; }

        /// <inheritdoc cref="ObjectsReader"/>
        public ObjectsReader(ReadOnlySpan<char> input, char[] leftQuotes, char[] rightQuotes)
        {
            this.leftQuotes = leftQuotes;
            this.rightQuotes = rightQuotes;
            this.input = input;
        }

        /// <summary>
        /// Read nex token.
        /// </summary>
        public bool Read()
        {
            // length of the current object read
            var dataLen = 0;

            var reachEndOfOneObject = false;
            var reachStartOfOneObject = false;

            var startPos = 0;

            // if we have only one left quote, we can set it
            if (this.leftQuotes.Length == 1)
                this.FirstLeftQuote = this.leftQuotes[0];

            // if we have only one right quote, we can set it
            if (this.rightQuotes.Length == 1)
                this.FirstRightQuote = this.rightQuotes[0];

            // iterate through the input
            while (this.dataPos <= this.input.Length - 1)
            {
                // if we reach the end of the input or the end of the current object
                if (this.dataPos == this.input.Length - 1 || (this.rightQuotes.Contains(this.input[this.dataPos]) && reachStartOfOneObject))
                {
                    // if we found a right quote and we did not determine the first quote, we can set it
                    if (this.FirstRightQuote == char.MinValue && this.rightQuotes.Contains(this.input[this.dataPos]))
                        this.FirstRightQuote = this.input[this.dataPos];

                    reachEndOfOneObject = true;

                    // if we are at the end of the input and the current character is not a right quote, we can add it to the current object
                    if (!this.rightQuotes.Contains(this.input[this.dataPos]) && this.input[this.dataPos] != '.' && this.input[this.dataPos] != ' ')
                        dataLen++;

                    // we have reached the end of the current object. We can move 1 forward to get the next object
                    if (this.rightQuotes.Contains(this.input[this.dataPos]) && this.dataPos < this.input.Length - 1)
                        this.dataPos++;
                }

                // if we found a special character like ".", we can skip it
                else if (this.input[this.dataPos] == '.')
                {

                    // if we are in progress of reading an object, we can skip this character and continue
                    if (reachStartOfOneObject && dataLen > 0)
                    {
                        reachEndOfOneObject = true;
                    }
                    else
                    {
                        // skip this character and start on the next one
                        this.dataPos++;
                        continue;
                    }
                }

                // if we found a left quote, we can start reading the current object
                else if (this.leftQuotes.Contains(this.input[this.dataPos]))
                {
                    if (this.FirstLeftQuote == char.MinValue && this.leftQuotes.Contains(this.input[this.dataPos]))
                        this.FirstLeftQuote = this.input[this.dataPos];

                    reachStartOfOneObject = true;

                    // omits this character
                    this.dataPos++;
                    startPos = this.dataPos;
                    dataLen = 0;
                }

                // we are hitting a classic character but never starts a new object. So we do it here
                if (!reachStartOfOneObject)
                {
                    reachStartOfOneObject = true;

                    // do not omit this character and start counting
                    startPos = this.dataPos;
                    dataLen = 1;
                }

                // we reach the end of the current object and we have read some data
                else if (reachEndOfOneObject && dataLen > 0)
                {
                    // we have reached the end of the current object
                    // we need to slice the input to get the object
                    this.Current = this.input.Slice(startPos, dataLen);
                    return true;
                }
                else
                {
                    dataLen++;
                }

                this.dataPos++;
            }

            return false;
        }
    }
}