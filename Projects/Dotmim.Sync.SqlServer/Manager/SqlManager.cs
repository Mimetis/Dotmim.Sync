using Dotmim.Sync.Core.Manager;
using System;
using System.Collections.Generic;
using System.Text;
using System.Data.Common;
using System.Data.SqlClient;

namespace Dotmim.Sync.SqlServer.Manager
{
    public class SqlManager : DbManager
    {

        SqlManagerTable tableManager;

        public SqlManager(string tableName): base(tableName)
        {

        }

        public override IDbManagerTable CreateManagerTable(DbConnection connection, DbTransaction transaction = null)
        {
            return new SqlManagerTable(connection, transaction);
        }

      
    }
}
