using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Filter
{
    public class SyncParameterCollection : List<SyncParameter>
    {


        public SyncParameterCollection()
        {

        }
        public void Add<T>(string tableName, string columnName, T value)
        {
            this.Add(new SyncParameter(tableName, columnName, value));
        }

    }
}
