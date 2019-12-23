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

        public static ParserName Parse(SyncTable input) => new ParserName(input);
        public static ParserName Parse(SyncTable input, string quote) => new ParserName(input, quote);
        public static ParserName Parse(SyncColumn input) => new ParserName(input);
        public static ParserName Parse(SyncColumn input, string quote) => new ParserName(input, quote);


        public static ParserName Parse(DmTable input) => new ParserName(input);
        public static ParserName Parse(DmTable input, string quote) => new ParserName(input, quote);
        public static ParserName Parse(string input) => new ParserName(input);
        public static ParserName Parse(string input, string quote) => new ParserName(input, quote);
        public static ParserName Parse(DmColumn input) => new ParserName(input);
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


        private ParserName(SyncTable table)
        {
            this.ParseSchemaTable(table);
        }

        private ParserName(SyncTable table, string quote)
        {
            this.quotePrefix = quote;
            this.quoteSuffix = quote;

            this.ParseSchemaTable(table);
        }

        private ParserName(SyncColumn column)
        {
            this.ParseSchemaColumn(column);
        }

        private ParserName(SyncColumn column, string quote)
        {
            this.quotePrefix = quote;
            this.quoteSuffix = quote;

            this.ParseSchemaColumn(column);
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

        private void ParseSchemaColumn(SyncColumn column)
        {
            // parse object name
            this.ParseObjectName(column.ColumnName);

            // parse schema
            if (!String.IsNullOrEmpty(column.Table?.SchemaName))
                this.ParseSchemaName(column.Table.SchemaName);

            // parse database name
            if (!String.IsNullOrEmpty(column.Table?.Schema?.DataSourceName))
                this.ParseDatabaseName(column.Table.Schema.DataSourceName);

        }
        private void ParseSchemaTable(SyncTable table)
        {
            // parse object name
            this.ParseObjectName(table.TableName);

            // parse schema
            if (!String.IsNullOrEmpty(table?.SchemaName))
                this.ParseSchemaName(table.SchemaName);

            // parse database name
            if (!String.IsNullOrEmpty(table.Schema?.DataSourceName))
                this.ParseDatabaseName(table.Schema.DataSourceName);

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
