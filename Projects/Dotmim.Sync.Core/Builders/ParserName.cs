
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
    /// <summary>
    /// ParserName is a class that helps to parse a string and get the database, schema and object name.
    /// </summary>
    public class ParserName
    {
        private string key;

        private bool withDatabase = false;
        private bool withSchema = false;
        private bool withQuotes = false;
        private bool withNormalized = false;

        /// <summary>
        /// Gets the schema name.
        /// </summary>
        public string SchemaName => GlobalParser.GetParserString(this.key).SchemaName;

        /// <summary>
        /// Gets the object name.
        /// </summary>
        public string ObjectName => GlobalParser.GetParserString(this.key).ObjectName;


        /// <summary>
        /// Gets the database name.
        /// </summary>
        public string DatabaseName => GlobalParser.GetParserString(this.key).DatabaseName;

        /// <summary>
        /// Add database name if available to the final string.
        /// </summary>
        public ParserName Database()
        {
            this.withDatabase = true;
            return this;
        }

        /// <summary>
        /// Add schema if available to the final string.
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


        private ParserName() { }

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

            GlobalParser.GetParserString(this.key);

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
            name = this.withNormalized ? name.Replace(" ", "_").Replace(".", "_").Replace("-", "_") : name;
            sb.Append(name);

            // now we have the correct string, reset options for the next time we call the same instance
            withDatabase = false;
            withSchema = false;
            withQuotes = false;
            withNormalized = false;

            return sb.ToString();


        }

        //public string ToString(bool addQuote = false, bool addSchema = false, bool isNormalized = false, bool addDatabase = false)
        //{
        //    var sb = new StringBuilder();

        //    var parsedName = GlobalParser.GetParserString(this.key);


        //    if (addDatabase && !string.IsNullOrEmpty(this.DatabaseName))
        //    {
        //        sb.Append(addQuote ? parsedName.QuotedDatabaseName : this.DatabaseName);
        //        sb.Append(isNormalized ? "_" : ".");
        //    }
        //    if (addSchema && !string.IsNullOrEmpty(this.SchemaName))
        //    {
        //        sb.Append(addQuote ? parsedName.QuotedSchemaName : this.SchemaName);
        //        sb.Append(isNormalized ? "_" : ".");
        //    }

        //    var name = addQuote ? parsedName.QuotedObjectName : this.ObjectName;
        //    name = isNormalized ? name.Replace(" ", "_").Replace(".", "_") : name;
        //    sb.Append(name);

        //    return sb.ToString();

        //}
    }

}
