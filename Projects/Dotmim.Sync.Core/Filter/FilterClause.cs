using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Filter
{
    /// <summary>
    /// Design a filter clause on Dmtable
    /// </summary>
    public class FilterClause
    {
        public String TableName { get; set; }

        public String ColumnName { get; set; }

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
