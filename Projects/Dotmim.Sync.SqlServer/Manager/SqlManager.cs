using Dotmim.Sync.Manager;
using System.Data.Common;

namespace Dotmim.Sync.SqlServer.Manager
{
    public class SqlManager : DbManager
    {

        public SqlManager(string tableName, string schemaName) : base(tableName, schemaName)
        {

        }

        public override IDbManagerTable CreateManagerTable(DbConnection connection, DbTransaction transaction = null)
        {
            return new SqlManagerTable(connection, transaction)
            {
                TableName = this.TableName,
                SchemaName = this.SchemaName
            };
        }


    }
}
