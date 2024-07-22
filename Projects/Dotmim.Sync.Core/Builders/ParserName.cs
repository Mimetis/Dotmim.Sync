using System.Text;

namespace Dotmim.Sync.Builders
{
    /// <summary>
    /// ParserName is a class that helps to parse a string and get the database, schema and object name.
    /// </summary>
    public class ParserName
    {
        private string key;

        private bool withDatabase;
        private bool withSchema;
        private bool withQuotes;
        private bool withNormalized;

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
        /// Parse a SyncTable and return a ParserName object.
        /// </summary>
        public static ParserName Parse(SyncTable syncTable, string leftQuote = null, string rightQuote = null) => new(syncTable, leftQuote, rightQuote);

        /// <summary>
        /// Parse a SyncColumn and return a ParserName object.
        /// </summary>
        public static ParserName Parse(SyncColumn syncColumn, string leftQuote = null, string rightQuote = null) => new(syncColumn, leftQuote, rightQuote);

        /// <summary>
        /// Parse a string and return a ParserName object.
        /// </summary>
        public static ParserName Parse(string input, string leftQuote = null, string rightQuote = null) => new(input, leftQuote, rightQuote);

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
        /// Add quotes ([] or ``) on all objects.
        /// </summary>
        public ParserName Quoted()
        {
            this.withQuotes = true;
            return this;
        }

        /// <summary>
        /// Remove quotes ([] or ``) on all objects.
        /// </summary>
        public ParserName Unquoted()
        {
            this.withQuotes = false;
            return this;
        }

        /// <summary>
        /// Normalize the object name (replace space, dot and - by _).
        /// </summary>
        public ParserName Normalized()
        {
            this.withNormalized = true;
            return this;
        }

        /// <summary>
        /// Returns the string representation of the object.
        /// </summary>
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
#if NET6_0_OR_GREATER
            name = this.withNormalized ? name.Replace(" ", "_", SyncGlobalization.DataSourceStringComparison).Replace(".", "_", SyncGlobalization.DataSourceStringComparison).Replace("-", "_", SyncGlobalization.DataSourceStringComparison) : name;
#else
            name = this.withNormalized ? name.Replace(" ", "_").Replace(".", "_").Replace("-", "_") : name;
#endif
            sb.Append(name);

            // now we have the correct string, reset options for the next time we call the same instance
            this.withDatabase = false;
            this.withSchema = false;
            this.withQuotes = false;
            this.withNormalized = false;

            return sb.ToString();
        }

        private ParserName()
        {
        }

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
        ///   "Fabrikam.[dbo].[client] === > Fabrikam dbo client ".
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
    }
}