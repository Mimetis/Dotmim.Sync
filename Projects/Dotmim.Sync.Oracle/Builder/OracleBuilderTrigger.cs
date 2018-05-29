using System.Data.Common;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;

namespace Dotmim.Sync.Oracle.Builder
{
    internal class OracleBuilderTrigger : IDbBuilderTriggerHelper
    {
        private DmTable tableDescription;
        private DbConnection connection;
        private DbTransaction transaction;

        public OracleBuilderTrigger(DmTable tableDescription, DbConnection connection, DbTransaction transaction)
        {
            this.tableDescription = tableDescription;
            this.connection = connection;
            this.transaction = transaction;
        }
    }
}