using Dotmim.Sync.Manager;
using System.Data.Common;

namespace Dotmim.Sync.SqlServer.Manager
{
    public class SqlManager : DbManager
    {

        public SqlManager(string tableName) : base(tableName)
        {

        }

        public override IDbManagerTable CreateManagerTable(DbConnection connection, DbTransaction transaction = null)
        {
            return new SqlManagerTable(connection, transaction)
            {
                TableName = this.TableName,
            };
        }


    }
}
