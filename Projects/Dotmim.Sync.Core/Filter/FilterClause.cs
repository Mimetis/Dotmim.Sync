using System;
using System.Data;

namespace Dotmim.Sync.Filter
{
    /// <summary>
    /// Design a filter clause on Dmtable
    /// </summary>
    [Serializable]
    public class FilterClause
    {
        public String TableName { get; set; }

        public String ColumnName { get; set; }

        public DbType? ColumnType { get; set; }

        /// <summary>
        /// Gets whether the filter is targeting an existing column of the target table (not virtual) or it is only used as a parameter in the selectchanges stored procedure (virtual)
        /// </summary>
        public bool IsVirtual => ColumnType.HasValue;
        
        /// <summary>
        /// Creates a filterclause allowing to specify a DbType.
        /// If you specify the columnType, Dotmim.Sync will expect that the column does not exist on the table, and the filter is only
        /// used as a parameter for the selectchanges stored procedure. Thus, IsVirtual would be true
        /// </summary>
        /// <param name="tableName">The table to be filtered</param>
        /// <param name="columnName">The name of the column - or the filterparameter</param>
        /// <param name="columnType">Pass null to filter on a real table column, pass a DbType in case the filter should only be a parameter in the selectchanges stored procedure</param>
        public FilterClause(string tableName, string columnName, DbType? columnType)
            : this(tableName, columnName)
        {
            ColumnType = columnType;
        }

        public FilterClause(string tableName, string columnName)
        {
            this.TableName = tableName;
            this.ColumnName = columnName;
        }

        public FilterClause()
        {
        }
    }
}
