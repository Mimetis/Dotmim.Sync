using Dotmim.Sync.Manager;
using System.Data.Common;

namespace Dotmim.Sync.Postgres.Manager
{
    public class NpgsqlManager : DbTableManagerFactory
    {

        public NpgsqlManager(string tableName, string schemaName) : base(tableName, schemaName)
        {

        }

        public override IDbTableManager CreateManagerTable(DbConnection connection, DbTransaction transaction = null)
        {
            return new NpgsqlManagerTable(connection, transaction)
            {
                TableName = this.TableName,
                SchemaName = this.SchemaName
            };
        }


    }
}
