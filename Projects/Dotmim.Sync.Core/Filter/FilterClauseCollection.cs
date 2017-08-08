using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Filter
{
    public class FilterClauseCollection : List<FilterClause>
    {
        private SyncConfiguration syncConfiguration;

        public FilterClauseCollection(SyncConfiguration syncConfiguration)
        {
            this.syncConfiguration = syncConfiguration;
        }

        public FilterClauseCollection()
        {
            
        }

        public void Add(string tableName, string columnName)
        {
            this.Add(new FilterClause(tableName, columnName));
        }

    }
}
