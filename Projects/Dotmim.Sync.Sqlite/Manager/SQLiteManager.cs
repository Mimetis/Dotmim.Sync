using Dotmim.Sync.Core.Manager;
using System;
using System.Collections.Generic;
using System.Text;
using System.Data.Common;


namespace Dotmim.Sync.SqlServer.Manager
{
    public class SQLiteManager : DbManager
    {

        SQLiteManagerTable tableManager;

        public SQLiteManager(string tableName): base(tableName)
        {

        }

        public override IDbManagerTable CreateManagerTable(DbConnection connection, DbTransaction transaction = null)
        {
            return new SQLiteManagerTable(connection, transaction);
        }

      
    }
}
