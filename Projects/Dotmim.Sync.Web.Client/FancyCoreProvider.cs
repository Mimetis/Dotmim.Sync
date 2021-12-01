using Dotmim.Sync.Builders;
using Dotmim.Sync.Manager;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync.Web.Client
{

    /// <summary>
    /// This provider is only here to be able to have a valid WebClientOrchestrator
    /// </summary>
    public class FancyCoreProvider : CoreProvider
    {
        public override DbMetadata GetMetadata() => throw new NotImplementedException();

        public override string GetProviderTypeName() => "Fancy";

        public override bool CanBeServerProvider => true;

         public override DbConnection CreateConnection() => throw new NotImplementedException();

        public override DbBuilder GetDatabaseBuilder() => throw new NotImplementedException();
        public override (ParserName tableName, ParserName trackingName) GetParsers(SyncTable tableDescription, SyncSetup setup) => throw new NotImplementedException();
        public override DbScopeBuilder GetScopeBuilder(string scope) => throw new NotImplementedException();
        public override DbSyncAdapter GetSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup) => throw new NotImplementedException();
        public override DbTableBuilder GetTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup) => throw new NotImplementedException();
    }
}
