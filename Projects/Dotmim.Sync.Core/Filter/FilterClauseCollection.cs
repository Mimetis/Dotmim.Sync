using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Core.Filter
{
    public class FilterClauseCollection : List<FilterClause>
    {
        private ServiceConfiguration serviceConfiguration;

        public FilterClauseCollection(ServiceConfiguration serviceConfiguration)
        {
            this.serviceConfiguration = serviceConfiguration;
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
