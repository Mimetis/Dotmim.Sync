
using System;
using System.Collections.Concurrent;
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
        private string key;

        private bool withDatabase = false;
        private bool withSchema = false;
        private bool withQuotes = false;
        private bool withNormalized = false;

        public string SchemaName => GlobalParser.GetParserString(this.key).SchemaName;
        public string ObjectName => GlobalParser.GetParserString(this.key).ObjectName;
        public string DatabaseName => GlobalParser.GetParserString(this.key).DatabaseName;

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

            var s = GlobalParser.GetParserString(this.key);

        }

        public override string ToString()
        {
            var sb = new StringBuilder();

            var parsedName = GlobalParser.GetParserString(this.key);


            if (this.withDatabase && !string.IsNullOrEmpty(this.DatabaseName))
            {
                sb.Append(this.withQuotes ? parsedName.QuotedDatabaseName : this.DatabaseName);
                sb.Append(this.withNormalized ? "_" : ".");
            }
            if (this.withSchema && !string.IsNullOrEmpty(this.SchemaName))
            {
                sb.Append(this.withQuotes ? parsedName.QuotedSchemaName : this.SchemaName);
                sb.Append(this.withNormalized ? "_" : ".");
            }

            var name = this.withQuotes ? parsedName.QuotedObjectName : this.ObjectName;
            name = this.withNormalized ? name.Replace(" ", "_").Replace(".", "_") : name;
            sb.Append(name);

            // now we have the correct string, reset options for the next time we call the same instance
            //withDatabase = false;
            //withSchema = false;
            //withQuotes = false;
            //withNormalized = false;

            return sb.ToString();


        }


    }

}
