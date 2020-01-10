using Dotmim.Sync.Manager;
using System.Data.Common;


namespace Dotmim.Sync.MySql
{
    public class MySqlManager : DbTableManagerFactory
    {
        public MySqlManager(string tableName) : base(tableName, string.Empty)
        {
        }

        public override IDbTableManager CreateManagerTable(DbConnection connection, DbTransaction transaction = null)
        {

            return new MySqlManagerTable(connection, transaction)
            {
                TableName = this.TableName,
            };
        }


    }
}
