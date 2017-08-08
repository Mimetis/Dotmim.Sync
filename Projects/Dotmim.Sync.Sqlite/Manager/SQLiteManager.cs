using Dotmim.Sync.Manager;
using System;
using System.Collections.Generic;
using System.Text;
using System.Data.Common;


namespace Dotmim.Sync.SQLite
{
    public class SQLiteManager : DbManager
    {
        public SQLiteManager(string tableName) : base(tableName)
        {

        }

        public override IDbManagerTable CreateManagerTable(DbConnection connection, DbTransaction transaction = null)
        {
            throw new NotImplementedException("At this time, SQLite does not support getting table structure from SQLite metadatas");
        }


    }
}
