using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Dotmim.Sync.Builders
{


    public class ParserName
    {

        // cache
        private static Dictionary<string, ParserString> parsers = new Dictionary<string, ParserString>();

        private string key;

        private bool withDatabase = false;
        private bool withSchema = false;
        private bool withQuotes = false;
        private bool withNormalized = false;

        public string SchemaName => parsers[key].SchemaName;
        public string ObjectName => parsers[key].ObjectName;
        public string DatabaseName => parsers[key].DatabaseName;

        /// <summary>
        /// Add database name if available to the final string
        /// </summary>
        public ParserName Database()
        {
            this.withDatabase = true;
            return this;

        }

        /// <summary>
        /// Add schema if available to the final string
        /// </summary>
        public ParserName Schema()
        {
            this.withSchema = true;
            return this;

        }

        /// <summary>
        /// Add quotes ([] or ``) on all objects 
        /// </summary>
        /// <returns></returns>
        public ParserName Quoted()
        {
            this.withQuotes = true;
            return this;
        }

        public ParserName Unquoted()
        {
            this.withQuotes = false;
            return this;

        }

        public ParserName Normalized()
        {
            this.withNormalized = true;
            return this;

        }

        public override string ToString()
        {
            var sb = new StringBuilder();

            if (this.withDatabase && !string.IsNullOrEmpty(this.DatabaseName))
            {
                sb.Append(this.withQuotes ? parsers[key].QuotedDatabaseName : this.DatabaseName);
                sb.Append(this.withNormalized ? "_" : ".");
            }
            if (this.withSchema && !string.IsNullOrEmpty(this.SchemaName))
            {
                sb.Append(this.withQuotes ? parsers[key].QuotedSchemaName : this.SchemaName);
                sb.Append(this.withNormalized ? "_" : ".");
            }

            var name = this.withQuotes ? parsers[key].QuotedObjectName : this.ObjectName;
            name = this.withNormalized ? name.Replace(" ", "_").Replace(".", "_") : name;
            sb.Append(name);

            // now we have the correct string, reset options for the next time we call the same instance
            withDatabase = false;
            withSchema = false;
            withQuotes = false;
            withNormalized = false;

            return sb.ToString();
        }

        public static ParserName Parse(SyncTable syncTable, string leftQuote = null, string rightQuote = null) => new ParserName(syncTable, leftQuote, rightQuote);
        public static ParserName Parse(SyncColumn syncColumn, string leftQuote = null, string rightQuote = null) => new ParserName(syncColumn, leftQuote, rightQuote);
        public static ParserName Parse(string input, string leftQuote = null, string rightQuote = null) => new ParserName(input, leftQuote, rightQuote);
   



        private ParserName(string input, string leftQuote = null, string rightQuote = null) => this.ParseString(input, leftQuote, rightQuote);
        private ParserName(SyncColumn column, string leftQuote = null, string rightQuote = null) => this.ParseString(column.ColumnName, leftQuote, rightQuote);
        private ParserName(SyncTable table, string leftQuote = null, string rightQuote = null)
        {
            string input = string.IsNullOrEmpty(table.SchemaName) ? table.TableName : $"{table.SchemaName}.{table.TableName}";
            this.ParseString(input, leftQuote, rightQuote);
        }



        /// <summary>
        /// Parse the input string and Get a non bracket object name :
        ///   "[Client] ==> Client "
        ///   "[dbo].[client] === > dbo client "
        ///   "dbo.client === > dbo client "
        ///   "Fabrikam.[dbo].[client] === > Fabrikam dbo client "
        /// </summary>
        private void ParseString(string input, string leftQuote = null, string rightQuote = null)
        {
            input = input == null ? string.Empty : input.Trim();
            this.key = input;

            if (!string.IsNullOrEmpty(leftQuote) && !string.IsNullOrEmpty(rightQuote))
                this.key = $"{leftQuote}^{rightQuote}^{input}";
            else if (!string.IsNullOrEmpty(leftQuote))
                this.key = $"{leftQuote}^{leftQuote}^{input}";

            // check cache
            if (parsers.ContainsKey(this.key))
                return;

            var parserString = new ParserString();
            parsers.Add(this.key, parserString);

            if (!string.IsNullOrEmpty(leftQuote))
            {
                parserString.QuotePrefix = leftQuote;
                parserString.QuoteSuffix = leftQuote;
            }
            if (!string.IsNullOrEmpty(rightQuote))
                parserString.QuoteSuffix = rightQuote;

            parserString.DatabaseName = string.Empty;
            parserString.QuotedDatabaseName = string.Empty;
            parserString.SchemaName = string.Empty;
            parserString.QuotedSchemaName = string.Empty;
            parserString.ObjectName = string.Empty;
            parserString.QuotedObjectName = string.Empty;

            //var regex = new Regex(string.Format(CultureInfo.InvariantCulture,
            //     "(?:(?<space>\\s+)*)*(?:(?<open>\\[)*)*(?(open)(?<quoted>[^\\]]+)*|(?<unquoted>[^\\.\\s\\]]+)*)",
            //     Regex.Escape(parserString.QuotePrefix), Regex.Escape(parserString.QuoteSuffix)), RegexOptions.IgnorePatternWhitespace);


            var regexExpression = string.Format(CultureInfo.InvariantCulture,
                     "(?<quoted>\\w[^\\{0}\\{1}\\.]*)",
                     parserString.QuotePrefix, parserString.QuoteSuffix);

            var regex = new Regex(regexExpression, RegexOptions.IgnorePatternWhitespace | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant); 


            var matchCollections = regex.Matches(input);

            string[] strMatches = new string[3];
            int matchCounts = 0;
            foreach (Match match in matchCollections)
            {
                if (matchCounts >= 3)
                    break;

                if (!match.Success || string.IsNullOrWhiteSpace(match.Value))
                    continue;

                var quotedGroup = match.Groups["quoted"];

                if (quotedGroup == null || string.IsNullOrEmpty(quotedGroup.Value))
                    continue;

                strMatches[matchCounts] = quotedGroup.Value.Trim();

                matchCounts++;
            }
            switch (matchCounts)
            {
                case 1:
                    {
                        this.ParseObjectName(parserString, strMatches[0]);
                        return;
                    }
                case 2:
                    {

                        this.ParseSchemaName(parserString, strMatches[0]);
                        this.ParseObjectName(parserString, strMatches[1]);
                        return;
                    }
                case 3:
                    {
                        this.ParseDatabaseName(parserString, strMatches[0]);
                        this.ParseSchemaName(parserString, strMatches[1]);
                        this.ParseObjectName(parserString, strMatches[2]);
                        return;
                    }
                default:
                    {
                        return;
                    }
            }
        }

        private void ParseObjectName(ParserString parserString, string name)
        {
            parserString.ObjectName = name;

            if (parserString.ObjectName.StartsWith(parserString.QuotePrefix))
                parserString.ObjectName = this.ObjectName.Substring(1);

            if (parserString.ObjectName.EndsWith(parserString.QuoteSuffix))
                parserString.ObjectName = parserString.ObjectName.Substring(0, parserString.ObjectName.Length - 1);

            parserString.QuotedObjectName = string.Concat(parserString.QuotePrefix, parserString.ObjectName, parserString.QuoteSuffix);

        }

        private void ParseSchemaName(ParserString parserString, string name)
        {
            parserString.SchemaName = name;

            if (!string.IsNullOrEmpty(parserString.SchemaName))
            {
                if (parserString.SchemaName.StartsWith(parserString.QuotePrefix))
                    parserString.SchemaName = this.SchemaName.Substring(1);

                if (parserString.SchemaName.EndsWith(parserString.QuoteSuffix))
                    parserString.SchemaName = parserString.SchemaName.Substring(0, parserString.SchemaName.Length - 1);

                parserString.QuotedSchemaName = string.Concat(parserString.QuotePrefix, this.SchemaName, parserString.QuoteSuffix);
            }
        }

        private void ParseDatabaseName(ParserString parserString, string name)
        {
            parserString.DatabaseName = name;

            if (parserString.DatabaseName.StartsWith(parserString.QuotePrefix))
                parserString.DatabaseName = parserString.DatabaseName.Substring(1);

            if (parserString.DatabaseName.EndsWith(parserString.QuoteSuffix))
                parserString.DatabaseName = parserString.DatabaseName.Substring(0, parserString.DatabaseName.Length - 1);

            parserString.QuotedDatabaseName = string.Concat(parserString.QuotePrefix, parserString.DatabaseName, parserString.QuoteSuffix);

        }
    }




}
