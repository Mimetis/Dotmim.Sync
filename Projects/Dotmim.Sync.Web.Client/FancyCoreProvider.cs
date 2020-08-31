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
        public override DbMetadata Metadata { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public override string ProviderTypeName => "Fancy";

        public override bool SupportBulkOperations => throw new NotImplementedException();

        public override bool CanBeServerProvider => true;

        public override DbConnection CreateConnection() => throw new NotImplementedException();

        public override DbBuilder GetDatabaseBuilder() => throw new NotImplementedException();

        public override DbScopeBuilder GetScopeBuilder() => throw new NotImplementedException();

        public override DbTableBuilder GetTableBuilder(SyncTable tableDescription, SyncSetup setup) => throw new NotImplementedException();

        public override DbTableManagerFactory GetTableManagerFactory(string tableName, string schemaName) => throw new NotImplementedException();
    }
}
