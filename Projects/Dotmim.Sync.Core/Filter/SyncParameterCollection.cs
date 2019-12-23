using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Filter
{
    [Serializable]
    public class SyncParameterCollection : List<SyncParameter>
    {
        public SyncParameterCollection()
        {

        }
        public void Add<T>(string tableName, string columnName, string schemaName, T value)
        {
            this.Add(new SyncParameter(tableName, columnName, schemaName, value));
        }

    }
}
