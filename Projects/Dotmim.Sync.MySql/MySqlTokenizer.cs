using System;
using System.Collections.Generic;
using System.Text;

#if MARIADB
namespace Dotmim.Sync.MariaDB
#elif MYSQL
namespace Dotmim.Sync.MySql
#endif
{
    internal class MySqlTokenizer
    {
        private string sql;

        private int startIndex;
        private int stopIndex;

        private bool ansiQuotes;
        private bool backslashEscapes;
        private bool returnComments;
        private bool multiLine;
        private bool sqlServerMode;

        private bool quoted;
        private bool isComment;

        private int pos;

        public MySqlTokenizer()
        {
            this.backslashEscapes = true;
            this.multiLine = true;
            this.pos = 0;
        }

        public MySqlTokenizer(string input)
          : this()
        {
            this.sql = input;
        }

        public string Text
        {
            get
            {
                return this.sql;
            }

            set
            {
                this.sql = value;
                this.pos = 0;
            }
        }

        public bool AnsiQuotes
        {
            get { return this.ansiQuotes; }
            set { this.ansiQuotes = value; }
        }

        public bool BackslashEscapes
        {
            get { return this.backslashEscapes; }
            set { this.backslashEscapes = value; }
        }

        public bool MultiLine
        {
            get { return this.multiLine; }
            set { this.multiLine = value; }
        }

        public bool SqlServerMode
        {
            get { return this.sqlServerMode; }
            set { this.sqlServerMode = value; }
        }

        public bool Quoted
        {
            get { return this.quoted; }
            private set { this.quoted = value; }
        }

        public bool IsComment
        {
            get { return this.isComment; }
        }

        public int StartIndex
        {
            get { return this.startIndex; }
            set { this.startIndex = value; }
        }

        public int StopIndex
        {
            get { return this.stopIndex; }
            set { this.stopIndex = value; }
        }

        public int Position
        {
            get { return this.pos; }
            set { this.pos = value; }
        }

        public bool ReturnComments
        {
            get { return this.returnComments; }
            set { this.returnComments = value; }
        }

        public static bool IsParameter(string s)
        {
            if (string.IsNullOrEmpty(s)) return false;
            if (s[0] == '?') return true;
            if (s.Length > 1 && s[0] == '@' && s[1] != '@') return true;
            return false;
        }

        public List<string> GetAllTokens()
        {
            List<string> tokens = new List<string>();
            string token = this.NextToken();
            while (token != null)
            {
                tokens.Add(token);
                token = this.NextToken();
            }

            return tokens;
        }

        public string NextToken()
        {
            while (this.FindToken())
            {
                string token = this.sql.Substring(this.startIndex, this.stopIndex - this.startIndex);
                return token;
            }

            return null;
        }

        public string NextParameter()
        {
            while (this.FindToken())
            {
                if ((this.stopIndex - this.startIndex) < 2) continue;
                char c1 = this.sql[this.startIndex];
                char c2 = this.sql[this.startIndex + 1];
                if (c1 == '?' ||
                    (c1 == '@' && c2 != '@'))
                    return this.sql.Substring(this.startIndex, this.stopIndex - this.startIndex);
            }

            return null;
        }

        public bool FindToken()
        {
            this.isComment = this.quoted = false;  // reset our flags
            this.startIndex = this.stopIndex = -1;

            while (this.pos < this.sql.Length)
            {
                char c = this.sql[this.pos++];
                if (char.IsWhiteSpace(c)) continue;

                if (c == '`' || c == '\'' || c == '"' || (c == '[' && this.SqlServerMode))
                {
                    this.ReadQuotedToken(c);
                }
                else if (c == '#' || c == '-' || c == '/')
                {
                    if (!this.ReadComment(c))
                        this.ReadSpecialToken();
                }
                else
                {
                    this.ReadUnquotedToken();
                }

                if (this.startIndex != -1) return true;
            }

            return false;
        }

        public string ReadParenthesis()
        {
            StringBuilder sb = new StringBuilder("(");
            int start = this.StartIndex;
            string token = this.NextToken();
            while (true)
            {
                if (token == null)
                    throw new InvalidOperationException("Unable to parse SQL");
                sb.Append(token);
                if (token == ")" && !this.Quoted) break;
                token = this.NextToken();
            }

            return sb.ToString();
        }

        private static bool IsQuoteChar(char c)
        {
            return c == '`' || c == '\'' || c == '\"';
        }

        private static bool IsParameterMarker(char c)
        {
            return c == '@' || c == '?';
        }

        private static bool IsSpecialCharacter(char c)
        {
            if (char.IsLetterOrDigit(c) ||
                c == '$' || c == '_' || c == '.') return false;
            if (IsParameterMarker(c)) return false;
            return true;
        }

        private bool ReadComment(char c)
        {
            // make sure the comment starts correctly
            if (c == '/' && (this.pos >= this.sql.Length || this.sql[this.pos] != '*')) return false;
            if (c == '-' && ((this.pos + 1) >= this.sql.Length || this.sql[this.pos] != '-' || this.sql[this.pos + 1] != ' ')) return false;

            string endingPattern = "\n";
            if (this.sql[this.pos] == '*')
                endingPattern = "*/";

            int startingIndex = this.pos - 1;

            int index = this.sql.IndexOf(endingPattern, this.pos);
            if (endingPattern == "\n")
                index = this.sql.IndexOf('\n', this.pos);
            if (index == -1)
                index = this.sql.Length - 1;
            else
                index += endingPattern.Length;

            this.pos = index;
            if (this.ReturnComments)
            {
                this.startIndex = startingIndex;
                this.stopIndex = index;
                this.isComment = true;
            }

            return true;
        }

        private void CalculatePosition(int start, int stop)
        {
            this.startIndex = start;
            this.stopIndex = stop;
            if (!this.MultiLine) return;
        }

        private void ReadUnquotedToken()
        {
            this.startIndex = this.pos - 1;

            if (!IsSpecialCharacter(this.sql[this.startIndex]))
            {
                while (this.pos < this.sql.Length)
                {
                    char c = this.sql[this.pos];
                    if (char.IsWhiteSpace(c)) break;
                    if (IsSpecialCharacter(c)) break;
                    this.pos++;
                }
            }

            this.Quoted = false;
            this.stopIndex = this.pos;
        }

        private void ReadSpecialToken()
        {
            this.startIndex = this.pos - 1;
            this.stopIndex = this.pos;
            this.Quoted = false;
        }

        /// <summary>
        ///  Read a single quoted identifier from the stream.
        /// </summary>
        private void ReadQuotedToken(char quoteChar)
        {
            if (quoteChar == '[')
                quoteChar = ']';
            this.startIndex = this.pos - 1;
            bool escaped = false;

            bool found = false;
            while (this.pos < this.sql.Length)
            {
                char c = this.sql[this.pos];

                if (c == quoteChar && !escaped)
                {
                    found = true;
                    break;
                }

                if (escaped)
                    escaped = false;
                else if (c == '\\' && this.BackslashEscapes)
                    escaped = true;
                this.pos++;
            }

            if (found) this.pos++;
            this.Quoted = found;
            this.stopIndex = this.pos;
        }
    }
}