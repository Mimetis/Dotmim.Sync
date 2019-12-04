using Dotmim.Sync.Manager;
using System.Data.Common;


namespace Dotmim.Sync.MySql
{
    public class MySqlManager : DbManager
    {
        public MySqlManager(string tableName) : base(tableName, "")
        {

        }

        public override IDbManagerTable CreateManagerTable(DbConnection connection, DbTransaction transaction = null)
        {
            return new MySqlManagerTable(connection, transaction)
            {
                TableName = this.TableName,
            };
        }


    }
}
