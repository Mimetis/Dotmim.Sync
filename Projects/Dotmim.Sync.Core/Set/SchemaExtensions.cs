using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;

namespace Dotmim.Sync
{
    /// <summary>
    /// Extensions methods for all Schema objects
    /// </summary>
    public static class SchemaExtensions
    {
 
        /// <summary>
        /// Compare two tables to see if they are equals
        /// </summary>
        public static bool StringEquals(this SyncSet schema, SyncTable table1, SyncTable table2)
        {
            var sc = schema.CaseSensitive ? StringComparison.InvariantCulture
                                         : StringComparison.InvariantCultureIgnoreCase;

            return string.Equals(table1.TableName, table2.TableName, sc)
                && string.Equals(table1.SchemaName, table2.SchemaName, sc);

        }

        /// <summary>
        /// Compare two tables to see if they are equals
        /// </summary>
        public static bool StringEquals(this SyncSet schema, string table1, string schema1, string table2, string schema2)
        {
            var sc = schema.CaseSensitive ? StringComparison.InvariantCulture
                                         : StringComparison.InvariantCultureIgnoreCase;

            var schema1Normalized = string.IsNullOrWhiteSpace(schema1) ? string.Empty : schema1;
            var schema2Normalized = string.IsNullOrWhiteSpace(schema2) ? string.Empty : schema2;


            return string.Equals(table1, table2, sc) && string.Equals(schema1Normalized, schema2Normalized, sc);

        }
        /// <summary>
        /// Compare two strings to see if they are equals
        /// </summary>
        public static bool StringEquals(this SyncSet schema, string s1, string s2)
        {
            //var parser1 = ParserName.Parse(s1);
            //var table1Name = parser1.ObjectName;
            //var schema1Name = parser1.SchemaName;

            //var parser2 = ParserName.Parse(s2);
            //var table2Name = parser2.ObjectName;
            //var schema2Name = parser2.SchemaName;

            var sc = schema.CaseSensitive ? StringComparison.InvariantCulture
                                         : StringComparison.InvariantCultureIgnoreCase;

            return string.Equals(s1, s2, sc);

        }


        /// <summary>
        /// Find all filters corresponding to a table
        /// </summary>
        public static IEnumerable<SyncFilter> Where(this SyncFilters filters, string tableName, string schemaName)
            => filters.Where(t => filters.Schema.StringEquals(tableName, schemaName, t.TableName, t.SchemaName));

        /// <summary>
        /// Find all filters corresponding to a table
        /// </summary>
        public static IEnumerable<SyncFilter> Where(this SyncFilters filters, SyncTable table)
            => filters.Where(table.TableName, table.SchemaName);

        /// <summary>
        /// Find all filters corresponding to a table
        /// </summary>
        public static IEnumerable<SyncFilter> Where(this SyncFilters filters, string table)
            => filters.Where(ParserName.Parse(table).ObjectName, ParserName.Parse(table).SchemaName);


        /// <summary>
        /// Find a table by its name / schema. Use schema case sensitive options to make the search
        /// </summary>
        public static SyncTable FirstOrDefault(this SyncTables tables, string tableName, string schemaName)
            => tables.FirstOrDefault(t => tables.Schema.StringEquals(tableName, schemaName, t.TableName, t.SchemaName));

        /// <summary>
        /// Find a table by its name / schema. Use schema case sensitive options to make the search 
        /// </summary>
        public static SyncTable FirstOrDefault(this SyncTables tables, SyncTable table)
            => tables.FirstOrDefault(table.TableName, table.SchemaName);

        /// <summary>
        /// Find a table by its name / schema. Use schema case sensitive options to make the search 
        /// </summary>
        public static SyncTable FirstOrDefault(this SyncTables tables, string tableName)
            => tables.FirstOrDefault(ParserName.Parse(tableName).ObjectName, ParserName.Parse(tableName).SchemaName);

        /// <summary>
        /// Find a table by its name / schema. Use schema case sensitive options to make the search
        /// </summary>
        public static SyncColumn FirstOrDefault(this SyncColumns columns, string columnName)
        {
            var sc = columns.Table.Schema.CaseSensitive ? StringComparison.InvariantCulture
                             : StringComparison.InvariantCultureIgnoreCase;

            return columns.FirstOrDefault(c => string.Equals(c.ColumnName, columnName, sc));

        }

     
    }
}
