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
        ///  Shortcut to add a new filter clause in a base list
        /// </summary>
        /// <param name="tableName">Table name involved in the filter</param>
        /// <param name="columnName">Column name involved in the filter</param>
        public static void Add(this ICollection<FilterClause> list, string tableName, string columnName)
        {
            list.Add(new FilterClause(tableName, columnName));
        }

        /// <summary>
        /// Shortcut to add a new virtualfilter clause in a base list.
        /// This filter must not exist as a column on the target table. It is only provided to the select_changs stored procedure
        /// </summary>
        /// <param name="tableName">Table name involved in the filter</param>
        /// <param name="columnName">Column name involved in the filter</param>
        /// <param name="type"></param>
        public static void Add(this ICollection<FilterClause> list, string tableName, string columnName, DbType type)
        {
            list.Add(new FilterClause(tableName, columnName, type));
        }

        /// <summary>
        ///  Shortcut to add a new parameter value to a filter
        /// </summary>
        /// <typeparam name="T">Parameter type</typeparam>
        /// <param name="tableName">Table name involved in the filter</param>
        /// <param name="columnName">Column name involved in the filter</param>
        /// <param name="value">Parameter value</param>
        public static void Add<T>(this ICollection<SyncParameter> paramsList, string tableName, string columnName, T value)
        {
            paramsList.Add(new SyncParameter(tableName, columnName, value));
        }

        /// <summary>
        ///  Shortcut to add a new parameter value to a filter
        /// </summary>
        /// <typeparam name="T">Parameter type</typeparam>
        /// <param name="filterClause">Filter clause, containing the table name and the column name</param>
        /// <param name="value">Parameter value</param>
        public static void Add<T>(this ICollection<SyncParameter> paramsList, FilterClause filterClause, T value)
        {
            paramsList.Add(new SyncParameter(filterClause.TableName, filterClause.ColumnName, value));
        }

        /// <summary>
        /// Returns a collection containing only FilterClauses which are not virtual (that are backed by a real table column)
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
        public static ICollection<FilterClause> GetColumnFilters(this ICollection<FilterClause> list)
        {
            if (list == null)
                return new List<FilterClause>();

            return list.Where(f => !f.IsVirtual).ToList();
        }

        /// <summary>
        /// Validates, that all column filters do refer to a an existing column of the target table
        /// </summary>
        /// <param name="filters"></param>
        /// <param name="tableDescription"></param>
        public static void ValidateColumnFilters(this ICollection<FilterClause> filters, DmTable tableDescription)
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
