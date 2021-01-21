using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Tools
{
    public class Argument
    {
        public Argument(ArgumentType argumentType, string term, string value, bool isLongTerm = false)
        {
            ArgumentType = argumentType;
            Term = term;
            Value = value;
            IsLongTerm = isLongTerm;
        }

        /// <summary>
        /// Gets or Stets the term used to call the argument
        /// </summary>
        public string Term { get; set; }

        /// <summary>
        /// Gets ors Sets a boolean indicating if this arg was called with short or long term
        /// </summary>
        public bool IsLongTerm { get; set; }

        /// <summary>
        /// Gets or Sets the Argument type
        /// </summary>
        internal ArgumentType ArgumentType { get; set; }

        /// <summary>
        /// Gets ors Sets the argument value
        /// </summary>
        public String Value { get; set; }

    }
}
