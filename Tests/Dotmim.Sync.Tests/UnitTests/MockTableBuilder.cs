using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class MockTableBuilder : DbTableBuilder
    {
        public MockTableBuilder(SyncTable tableDescription) : base(tableDescription)
        {
        }

        public override IDbBuilderProcedureHelper CreateProcBuilder(DbConnection connection, DbTransaction transaction = null)
            => new MockBuilderProcedureHelper();

        public override DbSyncAdapter CreateSyncAdapter(DbConnection connection, DbTransaction transaction = null)
            => new MockSyncAdapter(this.TableDescription, connection, transaction);

        public override IDbBuilderTableHelper CreateTableBuilder(DbConnection connection, DbTransaction transaction = null)
            => new MockBuilderTableHelper();

        public override IDbBuilderTrackingTableHelper CreateTrackingTableBuilder(DbConnection connection, DbTransaction transaction = null)
            => new MockBuilderTrackingTableHelper();

        public override IDbBuilderTriggerHelper CreateTriggerBuilder(DbConnection connection, DbTransaction transaction = null)
            => new MockBuilderTriggerHelper();
    }
}
