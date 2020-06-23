using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class MockTableBuilder : DbTableBuilder
    {
        public MockTableBuilder(SyncTable tableDescription, SyncSetup setup) : base(tableDescription, setup)
        {
        }

        public override IDbBuilderProcedureHelper CreateProcBuilder()
            => new MockBuilderProcedureHelper();

        public override DbSyncAdapter CreateSyncAdapter()
            => new MockSyncAdapter(this.TableDescription, this.Setup);

        public override IDbBuilderTableHelper CreateTableBuilder()
            => new MockBuilderTableHelper();

        public override IDbBuilderTrackingTableHelper CreateTrackingTableBuilder()
            => new MockBuilderTrackingTableHelper();

        public override IDbBuilderTriggerHelper CreateTriggerBuilder()
            => new MockBuilderTriggerHelper();

        public override (ParserName tableName, ParserName trackingName) GetParsers(SyncTable tableDescription, SyncSetup setup)
        {
            throw new NotImplementedException();
        }
    }
}
