using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Dotmim.Sync.Builders
{
    /// <summary>
    /// Parse a database object (like Fabrikam.dbo.Client or dbo.Client or [dbo].[Client] etc ...
    /// </summary>
    public class ObjectNameParser
    {
        /// <summary>
        /// Get or Set the prefix used (Default is "[")
        /// </summary>
        public string QuotePrefix { get; set; } = "[";

        /// <summary>
        /// Get or Set the suffix used (Default is "]")
        /// </summary>
        public string QuoteSuffix { get; set; } = "]";
        public string SchemaName { get; private set; }
        public string DatabaseName { get; private set; }
        public string ObjectName { get; private set; }
        public string QuotedDatabaseName { get; private set; }
        public string QuotedObjectName { get; private set; }
        public string QuotedSchemaName { get; private set; }
        public string QuotedString
        {
            get
            {
                StringBuilder stringBuilder = new StringBuilder();

                if (!string.IsNullOrEmpty(this.DatabaseName))
                {
                    stringBuilder.Append(this.QuotedDatabaseName);
                    stringBuilder.Append(".");
                }

                if (!string.IsNullOrEmpty(this.SchemaName))
                {
                    stringBuilder.Append(this.QuotedSchemaName);
                    stringBuilder.Append(".");
                }
                else if (!string.IsNullOrEmpty(this.DatabaseName) && !string.IsNullOrEmpty(this.ObjectName))
                {
                    // Double .. when we have a database and a table without schema
                    // Fabrikam..Client (instead of Fabrikam.dbo.Client)
                    stringBuilder.Append(".");
                }
                if (!string.IsNullOrEmpty(this.ObjectName))
                {
                    stringBuilder.Append(this.QuotedObjectName);
                }
                return stringBuilder.ToString();
            }
        }
        public string UnquotedString
        {
            get
            {
                StringBuilder stringBuilder = new StringBuilder();
                if (!string.IsNullOrEmpty(this.DatabaseName))
                {
                    stringBuilder.Append(this.DatabaseName);
                    stringBuilder.Append(".");
                }
                if (!string.IsNullOrEmpty(this.SchemaName))
                {
                    stringBuilder.Append(this.SchemaName);
                    stringBuilder.Append(".");
                }
                else if (!string.IsNullOrEmpty(this.DatabaseName) && !string.IsNullOrEmpty(this.ObjectName))
                {
                    stringBuilder.Append(".");
                }
                if (!string.IsNullOrEmpty(this.ObjectName))
                {
                    stringBuilder.Append(this.ObjectName);
                }
                return stringBuilder.ToString();
            }
        }
        public string UnquotedStringWithUnderScore
        {
            get
            {
                StringBuilder stringBuilder = new StringBuilder();
                if (!string.IsNullOrEmpty(this.DatabaseName))
                {
                    stringBuilder.Append(this.DatabaseName);
                    stringBuilder.Append("_");
                }
                if (!string.IsNullOrEmpty(this.SchemaName))
                {
                    stringBuilder.Append(this.SchemaName);
                    stringBuilder.Append("_");
                }
                else if (!string.IsNullOrEmpty(this.DatabaseName) && !string.IsNullOrEmpty(this.ObjectName))
                {
                    stringBuilder.Append("_");
                }
                if (!string.IsNullOrEmpty(this.ObjectName))
                {
                    stringBuilder.Append(this.ObjectName);
                }
                return stringBuilder.ToString();
            }
        }

        public ObjectNameParser()
        {
        }

        public ObjectNameParser(string input)
        {
            this.ParseString(input);
        }

        public ObjectNameParser(string quotePrefix, string quoteSuffix)
        {
            this.QuotePrefix = quotePrefix;
            this.QuoteSuffix = quoteSuffix;
        }

        public ObjectNameParser(string input, string quotePrefix, string quoteSuffix)
        {
            this.QuotePrefix = quotePrefix;
            this.QuoteSuffix = quoteSuffix;
            this.ParseString(input);
        }

        /// <summary>
        /// Parse the input string and Get a non bracket object name :
        ///   "[Client] ==> Client "
        ///   "[dbo].[client] === > dbo client "
        ///   "dbo.client === > dbo client "
        ///   "Fabrikam.[dbo].[client] === > Fabrikam dbo client "
        /// </summary>
        public void ParseString(string input)
        {
            this.DatabaseName = string.Empty;
            this.QuotedDatabaseName = string.Empty;
            this.SchemaName = string.Empty;
            this.QuotedSchemaName = string.Empty;
            this.ObjectName = string.Empty;
            this.QuotedObjectName = string.Empty;

            Regex regex = new Regex(string.Format(CultureInfo.InvariantCulture,
                 "(?:(?<space>\\s+)*)*(?:(?<open>\\[)*)*(?(open)(?<quoted>[^\\]]+)*|(?<unquoted>[^\\.\\s\\]]+)*)",
                 Regex.Escape(this.QuotePrefix), Regex.Escape(this.QuoteSuffix)), RegexOptions.IgnorePatternWhitespace);

            MatchCollection matchCollections = regex.Matches(input);

            string[] strMatches = new string[3];
            int matchCounts = 0;
            foreach (Match match in matchCollections)
            {
                if (matchCounts >= 3)
                    break;

                if (!match.Success || string.IsNullOrWhiteSpace(match.Value))
                    continue;

                strMatches[matchCounts] = (string.IsNullOrEmpty(match.Groups["quoted"].ToString()) ? match.Groups["unquoted"].Value : match.Groups["quoted"].Value);
                matchCounts++;
            }
            switch (matchCounts)
            {
                case 1:
                    {
                        this.ObjectName = strMatches[0];
                        if (this.ObjectName.StartsWith(this.QuotePrefix))
                            this.ObjectName = this.ObjectName.Substring(1);
                        if (this.ObjectName.EndsWith(this.QuotePrefix))
                            this.ObjectName = this.ObjectName.Substring(0, this.ObjectName.Length -1);

                        this.QuotedObjectName = string.Concat(this.QuotePrefix, this.ObjectName, this.QuoteSuffix);
                        return;
                    }
                case 2:
                    {
                        this.SchemaName = strMatches[0];
                        if (this.SchemaName.StartsWith(this.QuotePrefix))
                            this.SchemaName = this.SchemaName.Substring(1);
                        if (this.SchemaName.EndsWith(this.QuotePrefix))
                            this.SchemaName = this.SchemaName.Substring(0, this.SchemaName.Length - 1);
                        this.QuotedSchemaName = string.Concat(this.QuotePrefix, this.SchemaName, this.QuoteSuffix);

                        this.ObjectName = strMatches[1];
                        if (this.ObjectName.StartsWith(this.QuotePrefix))
                            this.ObjectName = this.ObjectName.Substring(1);
                        if (this.ObjectName.EndsWith(this.QuotePrefix))
                            this.ObjectName = this.ObjectName.Substring(0, this.ObjectName.Length - 1);
                        this.QuotedObjectName = string.Concat(this.QuotePrefix, this.ObjectName, this.QuoteSuffix);
                        return;
                    }
                case 3:
                    {
                        this.DatabaseName = strMatches[0];
                        if (this.DatabaseName.StartsWith(this.QuotePrefix))
                            this.DatabaseName = this.DatabaseName.Substring(1);
                        if (this.DatabaseName.EndsWith(this.QuotePrefix))
                            this.DatabaseName = this.DatabaseName.Substring(0, this.DatabaseName.Length - 1);
                        this.QuotedDatabaseName = string.Concat(this.QuotePrefix, this.DatabaseName, this.QuoteSuffix);

                        this.SchemaName = strMatches[1];
                        if (this.SchemaName.StartsWith(this.QuotePrefix))
                            this.SchemaName = this.SchemaName.Substring(1);
                        if (this.SchemaName.EndsWith(this.QuotePrefix))
                            this.SchemaName = this.SchemaName.Substring(0, this.SchemaName.Length - 1);
                        this.QuotedSchemaName = string.Concat(this.QuotePrefix, this.SchemaName, this.QuoteSuffix);

                        this.ObjectName = strMatches[2];
                        if (this.ObjectName.StartsWith(this.QuotePrefix))
                            this.ObjectName = this.ObjectName.Substring(1);
                        if (this.ObjectName.EndsWith(this.QuotePrefix))
                            this.ObjectName = this.ObjectName.Substring(0, this.ObjectName.Length - 1);
                        this.QuotedObjectName = string.Concat(this.QuotePrefix, this.ObjectName, this.QuoteSuffix);
                        return;
                    }
                default:
                    {
                        return;
                    }
            }
        }

        public override string ToString()
        {
            return this.QuotedString;
        }
    }
}
