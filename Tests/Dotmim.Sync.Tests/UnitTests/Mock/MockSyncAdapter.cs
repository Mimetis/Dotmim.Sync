using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class MockSyncAdapter : DbSyncAdapter
    {
        private DbConnection connection;
        private DbTransaction transaction;

        public MockSyncAdapter(SyncTable tableDescription, SyncSetup setup, DbConnection connection, DbTransaction transaction)
            : base(tableDescription, setup)
        {
            this.connection = connection;
            this.transaction = transaction;
        }

        public override DbConnection Connection => this.connection;

        public override DbTransaction Transaction => this.transaction;

        public override Task ExecuteBatchCommandAsync(DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> arrayItems, SyncTable schemaChangesTable, SyncTable failedRows, long lastTimestamp)
            => Task.CompletedTask;

        public override DbCommand GetCommand(DbCommandType commandType, SyncFilter filter = null)
            => new SqlCommand();

        public override bool IsPrimaryKeyViolation(Exception exception) => false;

        public override bool IsUniqueKeyViolation(Exception exception) => false;

        public override Task SetCommandParametersAsync(DbCommandType commandType, DbCommand command, SyncFilter filter = null)
            => Task.CompletedTask;
    }
}
