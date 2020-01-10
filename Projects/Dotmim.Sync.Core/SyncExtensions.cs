using Dotmim.Sync.Filter;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Dotmim.Sync.Data;

namespace Dotmim.Sync
{
    /// <summary>
    /// Contains only extensions methods
    /// </summary>
    public static class SyncExtensions
    {
        /// <summary>
        ///  Shortcut to add a new parameter value to a filter
        /// </summary>
        /// <typeparam name="T">Parameter type</typeparam>
        /// <param name="tableName">Table name involved in the filter</param>
        /// <param name="columnName">Column name involved in the filter</param>
        /// <param name="value">Parameter value</param>
        public static void Add<T>(this ICollection<SyncParameter> paramsList, string tableName, string columnName, string schemaName, T value)
        {
            paramsList.Add(new SyncParameter(tableName, columnName, schemaName, value));
        }

        /// <summary>
        /// Returns a collection containing only FilterClauses which are not virtual (that are backed by a real table column)
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
        public static ICollection<SyncFilter> GetColumnFilters(this ICollection<SyncFilter> list)
        {
            if (list == null)
                return new List<SyncFilter>();

            return list.Where(f => !f.IsVirtual).ToList();
        }

        /// <summary>
        /// Validates, that all column filters do refer to a an existing column of the target table
        /// </summary>
        /// <param name="filters"></param>
        /// <param name="tableDescription"></param>
        public static void ValidateColumnFilters(this SyncFilters filters, SyncTable tableDescription)
        {
            if (filters == null)
                return;

            foreach (var c in filters)
            {
                if (c.IsVirtual)
                    continue;

                var columnFilter = tableDescription.Columns[c.ColumnName];

                if (columnFilter == null)
                    throw new InvalidExpressionException($"Column {c.ColumnName} does not exist in Table {tableDescription.TableName}");
            }
        }
    }
}
