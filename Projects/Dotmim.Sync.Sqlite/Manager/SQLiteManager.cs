using Dotmim.Sync.Manager;
using System;
using System.Collections.Generic;
using System.Text;
using System.Data.Common;


namespace Dotmim.Sync.Sqlite
{
    public class SqliteManager : DbManager
    {
        public SqliteManager(string tableName) : base(tableName, "")
        {

        }

        public override IDbManagerTable CreateManagerTable(DbConnection connection, DbTransaction transaction = null)
        {
            // TODO : works with PRAGMA table_info('TableNAme');
            Console.WriteLine("At this time, Sqlite does not support getting table structure from Sqlite metadatas");
            return null;
        }


    }
}
