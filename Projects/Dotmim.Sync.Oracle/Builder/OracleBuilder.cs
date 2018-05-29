using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync.Oracle.Builder
{
    public class OracleBuilder : DbBuilder
    {
        public OracleBuilder(DmTable tableDescription) 
            : base(tableDescription)
        {
        }

        public override IDbBuilderProcedureHelper CreateProcBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new OracleBuilderProcedure(TableDescription, connection, transaction);
        }

        public override DbSyncAdapter CreateSyncAdapter(DbConnection connection, DbTransaction transaction = null)
        {
            return new OracleSyncAdapter(TableDescription, connection, transaction);
        }

        public override IDbBuilderTableHelper CreateTableBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new OracleBuilderTable(TableDescription, connection, transaction);
        }

        public override IDbBuilderTrackingTableHelper CreateTrackingTableBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new OracleBuilderTrackingTable(TableDescription, connection, transaction);
        }

        public override IDbBuilderTriggerHelper CreateTriggerBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new OracleBuilderTrigger(TableDescription, connection, transaction);
        }
    }
}
