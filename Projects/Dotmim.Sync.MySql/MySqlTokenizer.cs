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

        public MySqlTokenizer()
        {
            this.BackslashEscapes = true;
            this.MultiLine = true;
            this.Position = 0;
        }

        public MySqlTokenizer(string input)
          : this() => this.sql = input;

        public string Text
        {
            get => this.sql;

            set
            {
                this.sql = value;
                this.Position = 0;
            }
        }

        public bool AnsiQuotes { get; set; }

        public bool BackslashEscapes { get; set; }

        public bool MultiLine { get; set; }

        public bool SqlServerMode { get; set; }

        public bool Quoted { get; private set; }

        public bool IsComment { get; private set; }

        public int StartIndex { get; set; }

        public int StopIndex { get; set; }

        public int Position { get; set; }

        public bool ReturnComments { get; set; }

        public static bool IsParameter(string s) => !string.IsNullOrEmpty(s) && (s[0] == '?' || (s.Length > 1 && s[0] == '@' && s[1] != '@'));

        public List<string> GetAllTokens()
        {
            List<string> tokens = [];
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
                string token = this.sql.Substring(this.StartIndex, this.StopIndex - this.StartIndex);
                return token;
            }

            return null;
        }

        public string NextParameter()
        {
            while (this.FindToken())
            {
                if ((this.StopIndex - this.StartIndex) < 2) continue;
                char c1 = this.sql[this.StartIndex];
                char c2 = this.sql[this.StartIndex + 1];
                if (c1 == '?' ||
                    (c1 == '@' && c2 != '@'))
                    return this.sql.Substring(this.StartIndex, this.StopIndex - this.StartIndex);
            }

            return null;
        }

        public bool FindToken()
        {
            this.IsComment = this.Quoted = false;  // reset our flags
            this.StartIndex = this.StopIndex = -1;

            while (this.Position < this.sql.Length)
            {
                char c = this.sql[this.Position++];
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

                if (this.StartIndex != -1) return true;
            }

            return false;
        }

        public string ReadParenthesis()
        {
            StringBuilder sb = new StringBuilder("(");
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

        private static bool IsParameterMarker(char c) => c == '@' || c == '?';

        private static bool IsSpecialCharacter(char c) => !char.IsLetterOrDigit(c) &&
                c != '$' && c != '_' && c != '.' && !IsParameterMarker(c);

        private bool ReadComment(char c)
        {
            // make sure the comment starts correctly
            if (c == '/' && (this.Position >= this.sql.Length || this.sql[this.Position] != '*')) return false;
            if (c == '-' && ((this.Position + 1) >= this.sql.Length || this.sql[this.Position] != '-' || this.sql[this.Position + 1] != ' ')) return false;

            string endingPattern = "\n";
            if (this.sql[this.Position] == '*')
                endingPattern = "*/";

            int startingIndex = this.Position - 1;

            int index = this.sql.IndexOf(endingPattern, this.Position);
            if (endingPattern == "\n")
                index = this.sql.IndexOf('\n', this.Position);
            if (index == -1)
                index = this.sql.Length - 1;
            else
                index += endingPattern.Length;

            this.Position = index;
            if (this.ReturnComments)
            {
                this.StartIndex = startingIndex;
                this.StopIndex = index;
                this.IsComment = true;
            }

            return true;
        }

        private void ReadUnquotedToken()
        {
            this.StartIndex = this.Position - 1;

            if (!IsSpecialCharacter(this.sql[this.StartIndex]))
            {
                while (this.Position < this.sql.Length)
                {
                    char c = this.sql[this.Position];
                    if (char.IsWhiteSpace(c)) break;
                    if (IsSpecialCharacter(c)) break;
                    this.Position++;
                }
            }

            this.Quoted = false;
            this.StopIndex = this.Position;
        }

        private void ReadSpecialToken()
        {
            this.StartIndex = this.Position - 1;
            this.StopIndex = this.Position;
            this.Quoted = false;
        }

        /// <summary>
        ///  Read a single quoted identifier from the stream.
        /// </summary>
        private void ReadQuotedToken(char quoteChar)
        {
            if (quoteChar == '[')
                quoteChar = ']';
            this.StartIndex = this.Position - 1;
            bool escaped = false;

            bool found = false;
            while (this.Position < this.sql.Length)
            {
                char c = this.sql[this.Position];

                if (c == quoteChar && !escaped)
                {
                    found = true;
                    break;
                }

                if (escaped)
                    escaped = false;
                else if (c == '\\' && this.BackslashEscapes)
                    escaped = true;
                this.Position++;
            }

            if (found) this.Position++;
            this.Quoted = found;
            this.StopIndex = this.Position;
        }
    }
}