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


        public SqlChangeTrackingBuilderTrigger(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
            : base(tableDescription, tableName, trackingName, setup)
        {
        }
        public override Task CreateDeleteTriggerAsync(DbConnection connection, DbTransaction transaction)
            => Task.CompletedTask;

        public override Task CreateInsertTriggerAsync(DbConnection connection, DbTransaction transaction)
            => Task.CompletedTask;

        public override Task CreateUpdateTriggerAsync(DbConnection connection, DbTransaction transaction)
            => Task.CompletedTask;

        public override Task DropDeleteTriggerAsync(DbConnection connection, DbTransaction transaction) 
            => Task.CompletedTask;

        public override Task DropInsertTriggerAsync(DbConnection connection, DbTransaction transaction) 
            => Task.CompletedTask;

        public override Task DropUpdateTriggerAsync(DbConnection connection, DbTransaction transaction) 
            => Task.CompletedTask;

    }
}
