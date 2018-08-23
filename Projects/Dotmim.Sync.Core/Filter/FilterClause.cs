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

        public SqlDbType? Type { get; set; }

        public bool IsVirtual => Type.HasValue;
        
        public FilterClause(string tableName, string columnName, SqlDbType? type)
            : this(tableName, columnName)
        {
            Type = type;
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
