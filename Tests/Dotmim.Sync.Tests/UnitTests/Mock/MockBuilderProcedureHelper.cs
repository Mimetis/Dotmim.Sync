using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class MockBuilderProcedureHelper : IDbBuilderProcedureHelper
    {
        public Task CreateBulkDeleteAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;

        public Task CreateBulkUpdateAsync(bool hasMutableColumns,DbConnection connection, DbTransaction transaction) => Task.CompletedTask;


        public Task CreateDeleteAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;


        public Task CreateDeleteMetadataAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;


        public Task CreateResetAsync(DbConnection connection, DbTransaction transaction)  => Task.CompletedTask;


        public Task CreateSelectIncrementalChangesAsync(SyncFilter filter,DbConnection connection, DbTransaction transaction)  => Task.CompletedTask;


        public Task CreateSelectInitializedChangesAsync(SyncFilter filter,DbConnection connection, DbTransaction transaction)  => Task.CompletedTask;


        public Task CreateSelectRowAsync(DbConnection connection, DbTransaction transaction)  => Task.CompletedTask;


        public Task CreateTVPTypeAsync(DbConnection connection, DbTransaction transaction)  => Task.CompletedTask;


        public Task CreateUpdateAsync(bool hasMutableColumns,DbConnection connection, DbTransaction transaction)  => Task.CompletedTask;


        public Task DropBulkDeleteAsync(DbConnection connection, DbTransaction transaction)  => Task.CompletedTask;


        public Task DropBulkUpdateAsync(DbConnection connection, DbTransaction transaction)  => Task.CompletedTask;


        public Task DropDeleteAsync(DbConnection connection, DbTransaction transaction)  => Task.CompletedTask;


        public Task DropDeleteMetadataAsync(DbConnection connection, DbTransaction transaction)  => Task.CompletedTask;


        public Task DropResetAsync(DbConnection connection, DbTransaction transaction)  => Task.CompletedTask;


        public Task DropSelectIncrementalChangesAsync(SyncFilter filter,DbConnection connection, DbTransaction transaction)  => Task.CompletedTask;


        public Task DropSelectInitializedChangesAsync(SyncFilter filter,DbConnection connection, DbTransaction transaction)  => Task.CompletedTask;


        public Task DropSelectRowAsync(DbConnection connection, DbTransaction transaction)  => Task.CompletedTask;


        public Task DropTVPTypeAsync(DbConnection connection, DbTransaction transaction)  => Task.CompletedTask;


        public Task DropUpdateAsync(DbConnection connection, DbTransaction transaction)  => Task.CompletedTask;


        public Task<bool> NeedToCreateProcedureAsync(DbCommandType commandName,DbConnection connection, DbTransaction transaction) => Task.FromResult(true);


        public Task<bool> NeedToCreateTypeAsync(DbCommandType typeName,DbConnection connection, DbTransaction transaction) => Task.FromResult(true);

    }
}
