using System;

namespace Dotmim.Sync.DatabaseStringParsers
{
    /// <summary>
    /// Read database objects from a string.
    /// </summary>
    public ref struct ObjectsReader
    {
        private readonly ReadOnlySpan<char> input;
        private readonly char leftQuote;
        private readonly char rightQuote;

        private int dataPos = 0;

        /// <summary>
        /// Gets get current object.
        /// </summary>
        public ReadOnlySpan<char> Current { get; private set; }

        /// <inheritdoc cref="ObjectsReader"/>
        public ObjectsReader(ReadOnlySpan<char> input, char leftQuote, char rightQuote)
        {
            this.leftQuote = leftQuote;
            this.rightQuote = rightQuote;
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

            // iterate through the input
            while (this.dataPos <= this.input.Length - 1)
            {
                // if we reach the end of the input or the end of the current object
                if (this.dataPos == this.input.Length - 1 || this.input[this.dataPos] == this.rightQuote)
                {
                    reachEndOfOneObject = true;

                    // if we are at the end of the input and the current character is not a right quote, we can add it to the current object
                    if (this.input[this.dataPos] != this.rightQuote)
                        dataLen++;
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
                else if (this.input[this.dataPos] == this.leftQuote)
                {
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