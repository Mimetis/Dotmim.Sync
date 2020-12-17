using Dotmim.Sync.Builders;
using Dotmim.Sync.Manager;
using System;
using System.Collections.Generic;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Text;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class MockProvider : CoreProvider
    {
        public override DbMetadata Metadata { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public override string ProviderTypeName => "MockProvider";

        public override bool SupportBulkOperations => true;

        public override bool CanBeServerProvider => true;

        public override DbConnection CreateConnection()
        {
            return new SqlConnection();
        }

        public override DbBuilder GetDatabaseBuilder()
        {
            throw new NotImplementedException();
        }

        public override (ParserName tableName, ParserName trackingName) GetParsers(SyncTable tableDescription, SyncSetup setup) => throw new NotImplementedException();
        public override DbScopeBuilder GetScopeBuilder(string n) => new MockScopeBuilder();
        public override DbSyncAdapter GetSyncAdapter(SyncTable tableDescription, SyncSetup setup) => throw new NotImplementedException();

        public override DbTableBuilder GetTableBuilder(SyncTable tableDescription, SyncSetup setup) => throw new NotImplementedException();

    }
}
