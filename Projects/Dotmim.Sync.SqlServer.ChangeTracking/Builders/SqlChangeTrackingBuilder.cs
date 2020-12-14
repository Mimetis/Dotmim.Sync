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
        //public SqlChangeTrackingBuilder(SyncTable tableDescription, SyncSetup setup) : base(tableDescription, setup)
        //{
        //}

        //public override IDbBuilderTrackingTableHelper CreateTrackingTableBuilder() 
        //    => new SqlChangeTrackingBuilderTrackingTable(TableDescription, this.TableName, this.TrackingTableName, Setup);

        //public override IDbBuilderStoreProcedureCommands GetStoredProceduresCommands() 
        //    => new SqlChangeTrackingBuilderProcedure(TableDescription, this.TableName, this.TrackingTableName, Setup);

        //public override IDbBuilderTriggerCommands GetTriggerCommands() 
        //    => new SqlChangeTrackingBuilderTrigger(TableDescription, this.TableName, this.TrackingTableName, Setup);
        public SqlChangeTrackingBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup) : base(tableDescription, tableName, trackingTableName, setup)
        {
        }
    }
}
