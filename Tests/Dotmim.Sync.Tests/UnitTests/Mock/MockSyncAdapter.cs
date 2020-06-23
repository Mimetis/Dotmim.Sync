using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class MockSyncAdapter : SyncAdapter
    {
        public MockSyncAdapter(SyncTable tableDescription, SyncSetup setup)
            : base(tableDescription, setup)
        {
        }

        public override Task AddCommandParametersAsync(DbCommandType commandType, DbCommand command, DbConnection connection, DbTransaction transaction = null, SyncFilter filter = null)
       => Task.CompletedTask;

        public override Task ExecuteBatchCommandAsync(DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> arrayItems, SyncTable schemaChangesTable, SyncTable failedRows, long lastTimestamp, DbConnection connection, DbTransaction transaction = null)
            => Task.CompletedTask;

        public override DbCommand GetCommand(DbCommandType commandType, SyncFilter filter = null)
            => new SqlCommand();

        public override bool IsPrimaryKeyViolation(Exception exception) => false;

        public override bool IsUniqueKeyViolation(Exception exception) => false;

      
    }
}
