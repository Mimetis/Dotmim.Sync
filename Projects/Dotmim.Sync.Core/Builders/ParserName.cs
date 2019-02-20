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
    /// <summary>
    /// Parse a database object (like Fabrikam.dbo.Client or dbo.Client or [dbo].[Client] etc ...
    /// </summary>
    //public class ObjectNameParser
    //{
    //    /// <summary>
    //    /// Get or Set the prefix used (Default is "[")
    //    /// </summary>
    //    public string QuotePrefix { get; set; } = "[";

    //    /// <summary>
    //    /// Get or Set the suffix used (Default is "]")
    //    /// </summary>
    //    public string QuoteSuffix { get; set; } = "]";
    //    public string SchemaName { get; private set; }
    //    public string DatabaseName { get; private set; }
    //    public string ObjectName { get; private set; }

    //    /// <summary>
    //    /// Get the Object name normalized. Replacing spaces and dot with underscore
    //    /// </summary>
    //    public string ObjectNameNormalized
    //    {
    //        get
    //        {
    //            return this.ObjectName.Replace(" ", "_").Replace(".", "_");

    //        }
    //    }
    //    public string QuotedDatabaseName { get; private set; }
    //    public string QuotedObjectName { get; private set; }
    //    public string QuotedSchemaName { get; private set; }
    //    public string FullQuotedString
    //    {
    //        get
    //        {
    //            StringBuilder stringBuilder = new StringBuilder();

    //            if (!string.IsNullOrEmpty(this.DatabaseName))
    //            {
    //                stringBuilder.Append(this.QuotedDatabaseName);
    //                stringBuilder.Append(".");
    //            }

    //            if (!string.IsNullOrEmpty(this.SchemaName))
    //            {
    //                stringBuilder.Append(this.QuotedSchemaName);
    //                stringBuilder.Append(".");
    //            }
    //            else if (!string.IsNullOrEmpty(this.DatabaseName) && !string.IsNullOrEmpty(this.ObjectName))
    //            {
    //                // Double .. when we have a database and a table without schema
    //                // Fabrikam..Client (instead of Fabrikam.dbo.Client)
    //                stringBuilder.Append(".");
    //            }
    //            if (!string.IsNullOrEmpty(this.ObjectName))
    //            {
    //                stringBuilder.Append(this.QuotedObjectName);
    //            }
    //            return stringBuilder.ToString();
    //        }
    //    }
    //    public string FullUnquotedString
    //    {
    //        get
    //        {
    //            StringBuilder stringBuilder = new StringBuilder();
    //            if (!string.IsNullOrEmpty(this.DatabaseName))
    //            {
    //                stringBuilder.Append(this.DatabaseName);
    //                stringBuilder.Append(".");
    //            }
    //            if (!string.IsNullOrEmpty(this.SchemaName))
    //            {
    //                stringBuilder.Append(this.SchemaName);
    //                stringBuilder.Append(".");
    //            }
    //            else if (!string.IsNullOrEmpty(this.DatabaseName) && !string.IsNullOrEmpty(this.ObjectName))
    //            {
    //                stringBuilder.Append(".");
    //            }
    //            if (!string.IsNullOrEmpty(this.ObjectName))
    //            {
    //                stringBuilder.Append(this.ObjectName.Replace(" ", "_"));
    //            }
    //            return stringBuilder.ToString();
    //        }
    //    }
    //    public string FullUnquotedStringWithUnderScore
    //    {
    //        get
    //        {
    //            StringBuilder stringBuilder = new StringBuilder();
    //            if (!string.IsNullOrEmpty(this.DatabaseName))
    //            {
    //                stringBuilder.Append(this.DatabaseName);
    //                stringBuilder.Append("_");
    //            }
    //            if (!string.IsNullOrEmpty(this.SchemaName))
    //            {
    //                stringBuilder.Append(this.SchemaName);
    //                stringBuilder.Append("_");
    //            }
    //            else if (!string.IsNullOrEmpty(this.DatabaseName) && !string.IsNullOrEmpty(this.ObjectName))
    //            {
    //                stringBuilder.Append("_");
    //            }
    //            if (!string.IsNullOrEmpty(this.ObjectName))
    //            {
    //                stringBuilder.Append(this.ObjectName.Replace(" ", "_"));
    //            }
    //            return stringBuilder.ToString();
    //        }
    //    }

    //    public ObjectNameParser()
    //    {
    //    }


    //    /// <summary>
    //    /// Parse a column. Will take care of spaces in column name (not replacing it with a schema)
    //    /// </summary>
    //    public ObjectNameParser(DmColumn column)
    //    {
    //        this.ObjectName = column.ColumnName;

    //        this.ParseObjectName(column.ColumnName);
    //        if (!String.IsNullOrEmpty(column.Table?.Schema))
    //            this.ParseSchemaName(column.Table.Schema);

    //    }

    //    /// <summary>
    //    /// Parse a column. Will take care of spaces in column name (not replacing it with a schema)
    //    /// </summary>
    //    public ObjectNameParser(DmColumn column, string quotePrefix, string quoteSuffix)
    //    {
    //        this.QuotePrefix = quotePrefix;
    //        this.QuoteSuffix = quoteSuffix;

    //        this.ParseObjectName(column.ColumnName);
    //        if (!String.IsNullOrEmpty(column.Table?.Schema))
    //            this.ParseSchemaName(column.Table.Schema);

    //    }




    //    /// <summary>
    //    /// Parse the input string and Get a non bracket object name :
    //    ///   "[Client] ==> Client "
    //    ///   "[dbo].[client] === > dbo client "
    //    ///   "dbo.client === > dbo client "
    //    ///   "Fabrikam.[dbo].[client] === > Fabrikam dbo client "
    //    /// </summary>
    //    public ObjectNameParser(string input)
    //    {
    //        this.ParseString(input);
    //    }

    //    /// <summary>
    //    /// Parse the input string and Get a non bracket object name :
    //    ///   "[Client] ==> Client "
    //    ///   "[dbo].[client] === > dbo client "
    //    ///   "dbo.client === > dbo client "
    //    ///   "Fabrikam.[dbo].[client] === > Fabrikam dbo client "
    //    /// </summary>
    //    public ObjectNameParser(string quotePrefix, string quoteSuffix)
    //    {
    //        this.QuotePrefix = quotePrefix;
    //        this.QuoteSuffix = quoteSuffix;
    //    }

    //    /// <summary>
    //    /// Parse the input string and Get a non bracket object name :
    //    ///   "[Client] ==> Client "
    //    ///   "[dbo].[client] === > dbo client "
    //    ///   "dbo.client === > dbo client "
    //    ///   "Fabrikam.[dbo].[client] === > Fabrikam dbo client "
    //    /// </summary>
    //    public ObjectNameParser(string input, string quotePrefix, string quoteSuffix)
    //    {
    //        this.QuotePrefix = quotePrefix;
    //        this.QuoteSuffix = quoteSuffix;
    //        this.ParseString(input);
    //    }

    //    public void ParseString(string input)
    //    {
    //        this.DatabaseName = string.Empty;
    //        this.QuotedDatabaseName = string.Empty;
    //        this.SchemaName = string.Empty;
    //        this.QuotedSchemaName = string.Empty;
    //        this.ObjectName = string.Empty;
    //        this.QuotedObjectName = string.Empty;

    //        Regex regex = new Regex(string.Format(CultureInfo.InvariantCulture,
    //             "(?:(?<space>\\s+)*)*(?:(?<open>\\[)*)*(?(open)(?<quoted>[^\\]]+)*|(?<unquoted>[^\\.\\s\\]]+)*)",
    //             Regex.Escape(this.QuotePrefix), Regex.Escape(this.QuoteSuffix)), RegexOptions.IgnorePatternWhitespace);

    //        MatchCollection matchCollections = regex.Matches(input);

    //        string[] strMatches = new string[3];
    //        int matchCounts = 0;
    //        foreach (Match match in matchCollections)
    //        {
    //            if (matchCounts >= 3)
    //                break;

    //            if (!match.Success || string.IsNullOrWhiteSpace(match.Value))
    //                continue;

    //            strMatches[matchCounts] = (string.IsNullOrEmpty(match.Groups["quoted"].ToString()) ? match.Groups["unquoted"].Value : match.Groups["quoted"].Value);
    //            matchCounts++;
    //        }
    //        switch (matchCounts)
    //        {
    //            case 1:
    //                {
    //                    this.ParseObjectName(strMatches[0]);
    //                    return;
    //                }
    //            case 2:
    //                {

    //                    this.ParseSchemaName(strMatches[0]);
    //                    this.ParseObjectName(strMatches[1]);
    //                    return;
    //                }
    //            case 3:
    //                {
    //                    this.DatabaseName = strMatches[0];
    //                    if (this.DatabaseName.StartsWith(this.QuotePrefix))
    //                        this.DatabaseName = this.DatabaseName.Substring(1);
    //                    if (this.DatabaseName.EndsWith(this.QuotePrefix))
    //                        this.DatabaseName = this.DatabaseName.Substring(0, this.DatabaseName.Length - 1);
    //                    this.QuotedDatabaseName = string.Concat(this.QuotePrefix, this.DatabaseName, this.QuoteSuffix);

    //                    this.ParseSchemaName(strMatches[1]);
    //                    this.ParseObjectName(strMatches[2]);
    //                    return;
    //                }
    //            default:
    //                {
    //                    return;
    //                }
    //        }
    //    }

    //    private void ParseObjectName(string name)
    //    {
    //        this.ObjectName = name;

    //        if (this.ObjectName.StartsWith(this.QuotePrefix))
    //            this.ObjectName = this.ObjectName.Substring(1);
    //        if (this.ObjectName.EndsWith(this.QuotePrefix))
    //            this.ObjectName = this.ObjectName.Substring(0, this.ObjectName.Length - 1);
    //        this.QuotedObjectName = string.Concat(this.QuotePrefix, this.ObjectName, this.QuoteSuffix);

    //    }

    //    private void ParseSchemaName(string name)
    //    {
    //        this.SchemaName = name;
    //        if (!String.IsNullOrEmpty(this.SchemaName))
    //        {
    //            if (this.SchemaName.StartsWith(this.QuotePrefix))
    //                this.SchemaName = this.SchemaName.Substring(1);
    //            if (this.SchemaName.EndsWith(this.QuotePrefix))
    //                this.SchemaName = this.SchemaName.Substring(0, this.SchemaName.Length - 1);
    //            this.QuotedSchemaName = string.Concat(this.QuotePrefix, this.SchemaName, this.QuoteSuffix);

    //        }
    //    }

    //    public override string ToString()
    //    {
    //        return this.FullQuotedString;
    //    }
    //}


    public class ParserName
    {
        private bool withDatabase = false;
        private bool withSchema = false;
        private bool withQuotes = false;
        private bool withNormalized = false;
        private string quotedSchemaName;
        private string quotedDatabaseName;
        private string quotedObjectName;

        private string quotePrefix = "[";
        private string quoteSuffix = "]";

        public string SchemaName { get; set; }
        public string ObjectName { get; set; }
        public string DatabaseName { get; set; }

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
                sb.Append(this.withQuotes ? this.quotedDatabaseName : this.DatabaseName);
                sb.Append(this.withNormalized ? "_" : ".");
            }
            if (this.withSchema && !string.IsNullOrEmpty(this.SchemaName))
            {
                sb.Append(this.withQuotes ? this.quotedSchemaName : this.SchemaName);
                sb.Append(this.withNormalized ? "_" : ".");
            }

            var name = this.withQuotes ? this.quotedObjectName : this.ObjectName;
            name = this.withNormalized ? name.Replace(" ", "_").Replace(".", "_") : name;
            sb.Append(name);

            // now we have the correct string, reset options for the next time we call the same instance
            withDatabase = false;
            withSchema = false;
            withQuotes = false;
            withNormalized = false;

            return sb.ToString();
        }

        public static ParserName Parse(DmTable input) => new ParserName(input);
        public static ParserName Parse(DmTable input, string quote) => new ParserName(input, quote);
        public static ParserName Parse(string input) => new ParserName(input);
        public static ParserName Parse(DmColumn input) => new ParserName(input);
        public static ParserName Parse(string input, string quote) => new ParserName(input, quote);
        public static ParserName Parse(DmColumn input, string quote) => new ParserName(input, quote);

        private ParserName(string input)
        {
            this.ParseString(input);
        }

        private ParserName(string input, string quote)
        {
            this.quotePrefix = quote;
            this.quoteSuffix = quote;

            this.ParseString(input);
        }

        private ParserName(DmColumn column)
        {
            this.ParseDmColumn(column);
        }

        private ParserName(DmColumn column, string quote)
        {
            this.quotePrefix = quote;
            this.quoteSuffix = quote;

            this.ParseDmColumn(column);
        }

        private ParserName(DmTable table)
        {
            this.ParseDmTable(table);
        }

        private ParserName(DmTable table, string quote)
        {
            this.quotePrefix = quote;
            this.quoteSuffix = quote;

            this.ParseDmTable(table);
        }

        private void ParseDmColumn(DmColumn column)
        {
            // parse object name
            this.ParseObjectName(column.ColumnName);

            // parse schema
            if (!String.IsNullOrEmpty(column.Table?.Schema))
                this.ParseSchemaName(column.Table.Schema);

            // parse database name
            if (!String.IsNullOrEmpty(column.Table?.DmSet?.DmSetName))
                this.ParseDatabaseName(column.Table.DmSet.DmSetName);

        }

        private void ParseDmTable(DmTable table)
        {
            // parse object name
            this.ParseObjectName(table.TableName);

            // parse schema
            if (!String.IsNullOrEmpty(table?.Schema))
                this.ParseSchemaName(table.Schema);

            // parse database name
            if (!String.IsNullOrEmpty(table.DmSet?.DmSetName))
                this.ParseDatabaseName(table.DmSet.DmSetName);

        }


        /// <summary>
        /// Parse the input string and Get a non bracket object name :
        ///   "[Client] ==> Client "
        ///   "[dbo].[client] === > dbo client "
        ///   "dbo.client === > dbo client "
        ///   "Fabrikam.[dbo].[client] === > Fabrikam dbo client "
        /// </summary>
        private void ParseString(string input)
        {
            this.DatabaseName = string.Empty;
            this.quotedDatabaseName = string.Empty;
            this.SchemaName = string.Empty;
            this.quotedSchemaName = string.Empty;
            this.ObjectName = string.Empty;
            this.quotedObjectName = string.Empty;

            var regex = new Regex(string.Format(CultureInfo.InvariantCulture,
                 "(?:(?<space>\\s+)*)*(?:(?<open>\\[)*)*(?(open)(?<quoted>[^\\]]+)*|(?<unquoted>[^\\.\\s\\]]+)*)",
                 Regex.Escape(this.quotePrefix), Regex.Escape(this.quoteSuffix)), RegexOptions.IgnorePatternWhitespace);

            var matchCollections = regex.Matches(input);

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
                        this.ParseObjectName(strMatches[0]);
                        return;
                    }
                case 2:
                    {

                        this.ParseSchemaName(strMatches[0]);
                        this.ParseObjectName(strMatches[1]);
                        return;
                    }
                case 3:
                    {
                        this.ParseDatabaseName(strMatches[0]);
                        this.ParseSchemaName(strMatches[1]);
                        this.ParseObjectName(strMatches[2]);
                        return;
                    }
                default:
                    {
                        return;
                    }
            }
        }

        private void ParseObjectName(string name)
        {
            this.ObjectName = name;

            if (this.ObjectName.StartsWith(this.quotePrefix))
                this.ObjectName = this.ObjectName.Substring(1);
            if (this.ObjectName.EndsWith(this.quotePrefix))
                this.ObjectName = this.ObjectName.Substring(0, this.ObjectName.Length - 1);
            this.quotedObjectName = string.Concat(this.quotePrefix, this.ObjectName, this.quoteSuffix);

        }

        private void ParseSchemaName(string name)
        {
            this.SchemaName = name;
            if (!String.IsNullOrEmpty(this.SchemaName))
            {
                if (this.SchemaName.StartsWith(this.quotePrefix))
                    this.SchemaName = this.SchemaName.Substring(1);
                if (this.SchemaName.EndsWith(this.quotePrefix))
                    this.SchemaName = this.SchemaName.Substring(0, this.SchemaName.Length - 1);
                this.quotedSchemaName = string.Concat(this.quotePrefix, this.SchemaName, this.quoteSuffix);

            }
        }

        private void ParseDatabaseName(string name)
        {
            this.DatabaseName = name;
            if (this.DatabaseName.StartsWith(this.quotePrefix))
                this.DatabaseName = this.DatabaseName.Substring(1);
            if (this.DatabaseName.EndsWith(this.quotePrefix))
                this.DatabaseName = this.DatabaseName.Substring(0, this.DatabaseName.Length - 1);
            this.quotedDatabaseName = string.Concat(this.quotePrefix, this.DatabaseName, this.quoteSuffix);

        }
    }
}
