using Dotmim.Sync.Manager;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync.Oracle.Manager
{
    public class OracleManager : DbManager
    {
        public OracleManager(string tableName) : base(tableName)
        {
        }

        public override IDbManagerTable CreateManagerTable(DbConnection connection, DbTransaction transaction = null)
        {
            return new OracleManagerTable(connection, transaction);
        }
    }
}
