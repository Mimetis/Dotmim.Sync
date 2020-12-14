
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.Manager
{
    public abstract class DbTableManagerFactory
    {

        public string TableName { get; }
        public string SchemaName { get; }

        public DbTableManagerFactory(string tableName, string schemaName)
        {
            this.TableName = tableName;
            this.SchemaName = schemaName;
        }

     


    }
}
