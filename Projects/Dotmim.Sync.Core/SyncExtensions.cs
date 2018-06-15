using Dotmim.Sync.Filter;
using System;
using System.Collections.Generic;
using System.Text;

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
    }
}
