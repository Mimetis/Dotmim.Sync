using Dotmim.Sync.Builders;
using Dotmim.Sync.SqlServer.Builders;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync.SqlServer.ChangeTracking.Builders
{
    public class SqlChangeTrackingBuilder : SqlTableBuilder
    {
        public SqlChangeTrackingBuilder(SyncTable tableDescription) : base(tableDescription)
        {
        }

        public override IDbBuilderTrackingTableHelper CreateTrackingTableBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new SqlChangeTrackingBuilderTrackingTable(TableDescription, connection, transaction);
        }

        public override IDbBuilderProcedureHelper CreateProcBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new SqlChangeTrackingBuilderProcedure(TableDescription, connection, transaction);
        }

        public override IDbBuilderTriggerHelper CreateTriggerBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new SqlChangeTrackingBuilderTrigger(TableDescription, connection, transaction);
        }

        
    }
}
