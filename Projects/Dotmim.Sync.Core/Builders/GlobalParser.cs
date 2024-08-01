using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.Text.RegularExpressions;

namespace Dotmim.Sync.Builders
{
    /// <summary>
    /// Global parser to parse a string and get the database, schema and object name.
    /// </summary>
    public static class GlobalParser
    {

        // 'GetOrAdd' call on the dictionary is not thread safe and we might end up creating the pipeline more
        // once. To prevent this Lazy<> is used. In the worst case multiple Lazy<> objects are created for multiple
        // threads but only one of the objects succeeds
        // See https://andrewlock.net/making-getoradd-on-concurrentdictionary-thread-safe-using-lazy/
        private static ConcurrentDictionary<string, Lazy<ParserString>> parsers = new();

        /// <summary>
        /// Get a ParserString thanks to the key. If not available, create a new ParserString and return it.
        /// </summary>
        /// <param name="key">key composed with leftQuotes^rightQuotes^input.</param>
        public static ParserString GetParserString(string key)
        {
            // Try to get the instance
            var parserStringRetrieved = parsers.GetOrAdd(key, k =>
                new Lazy<ParserString>(() => InternalParse(key)));

            return parserStringRetrieved.Value;
        }

        private static ParserString InternalParse(string key)
        {
            var t = key.Split('^');
            string leftQuote;
            string rightQuote;
            string input;
            if (t.Length == 1)
            {
                leftQuote = string.Empty;
                rightQuote = string.Empty;
                input = t[0];
            }
            else if (t.Length == 2)
            {
                leftQuote = t[0];
                rightQuote = t[0];
                input = t[1];
            }
            else if (t.Length == 3)
            {
                leftQuote = t[0];
                rightQuote = t[1];
                input = t[2];
            }
            else
            {
                throw new Exception("Lengh of Parser key splitted with ^ is invalid");
            }

            // Preparing a new instance
            var parserString = new ParserString();

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

            // var regex = new Regex(string.Format(CultureInfo.InvariantCulture,
            //     "(?:(?<space>\\s+)*)*(?:(?<open>\\[)*)*(?(open)(?<quoted>[^\\]]+)*|(?<unquoted>[^\\.\\s\\]]+)*)",
            //     Regex.Escape(parserString.QuotePrefix), Regex.Escape(parserString.QuoteSuffix)), RegexOptions.IgnorePatternWhitespace);
            var regexExpression = string.Format(
                CultureInfo.InvariantCulture,
                "(?<quoted>\\w[^\\{0}\\{1}\\.]*)",
                parserString.QuotePrefix, parserString.QuoteSuffix);

            var regex = new Regex(regexExpression, RegexOptions.IgnorePatternWhitespace | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

            var matchCollections = regex.Matches(input);

            var strMatches = new string[3];
            var matchCounts = 0;
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
                        ParseObjectName(parserString, strMatches[0]);
                        return parserString;
                    }

                case 2:
                    {

                        ParseSchemaName(parserString, strMatches[0]);
                        ParseObjectName(parserString, strMatches[1]);
                        return parserString;
                    }

                case 3:
                    {
                        ParseDatabaseName(parserString, strMatches[0]);
                        ParseSchemaName(parserString, strMatches[1]);
                        ParseObjectName(parserString, strMatches[2]);
                        return parserString;
                    }

                default:
                    {
                        return parserString;
                    }
            }
        }

        private static void ParseObjectName(ParserString parserString, string name)
        {
            parserString.ObjectName = name;

            if (parserString.ObjectName.StartsWith(parserString.QuotePrefix, SyncGlobalization.DataSourceStringComparison))
                parserString.ObjectName = parserString.ObjectName.Substring(1);

            if (parserString.ObjectName.EndsWith(parserString.QuoteSuffix, SyncGlobalization.DataSourceStringComparison))
                parserString.ObjectName = parserString.ObjectName.Substring(0, parserString.ObjectName.Length - 1);

            parserString.QuotedObjectName = string.Concat(parserString.QuotePrefix, parserString.ObjectName, parserString.QuoteSuffix);
        }

        private static void ParseSchemaName(ParserString parserString, string name)
        {
            parserString.SchemaName = name;

            if (!string.IsNullOrEmpty(parserString.SchemaName))
            {
                if (parserString.SchemaName.StartsWith(parserString.QuotePrefix, SyncGlobalization.DataSourceStringComparison))
                    parserString.SchemaName = parserString.SchemaName.Substring(1);

                if (parserString.SchemaName.EndsWith(parserString.QuoteSuffix, SyncGlobalization.DataSourceStringComparison))
                    parserString.SchemaName = parserString.SchemaName.Substring(0, parserString.SchemaName.Length - 1);

                parserString.QuotedSchemaName = string.Concat(parserString.QuotePrefix, parserString.SchemaName, parserString.QuoteSuffix);
            }
        }

        private static void ParseDatabaseName(ParserString parserString, string name)
        {
            parserString.DatabaseName = name;

            if (parserString.DatabaseName.StartsWith(parserString.QuotePrefix, SyncGlobalization.DataSourceStringComparison))
                parserString.DatabaseName = parserString.DatabaseName.Substring(1);

            if (parserString.DatabaseName.EndsWith(parserString.QuoteSuffix, SyncGlobalization.DataSourceStringComparison))
                parserString.DatabaseName = parserString.DatabaseName.Substring(0, parserString.DatabaseName.Length - 1);

            parserString.QuotedDatabaseName = string.Concat(parserString.QuotePrefix, parserString.DatabaseName, parserString.QuoteSuffix);
        }
    }
}