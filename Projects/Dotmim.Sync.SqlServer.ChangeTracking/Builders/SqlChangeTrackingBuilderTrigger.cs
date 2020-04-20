using Dotmim.Sync.Builders;



using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlChangeTrackingBuilderTrigger : SqlBuilderTrigger
    {


        public SqlChangeTrackingBuilderTrigger(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup, DbConnection connection, DbTransaction transaction = null)
            : base(tableDescription, tableName, trackingName, setup, connection, transaction)
        {
        }

        public override Task<bool> NeedToCreateTriggerAsync(DbTriggerType type) => Task.FromResult(false);

        public override Task DropDeleteTriggerAsync() => Task.CompletedTask;
        public override Task DropInsertTriggerAsync() => Task.CompletedTask;
        public override Task DropUpdateTriggerAsync() => Task.CompletedTask;

    }
}
