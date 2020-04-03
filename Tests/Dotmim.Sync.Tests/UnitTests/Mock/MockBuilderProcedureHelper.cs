using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class MockBuilderProcedureHelper : IDbBuilderProcedureHelper
    {
        public Task CreateBulkDeleteAsync() => Task.CompletedTask;

        public Task CreateBulkUpdateAsync(bool hasMutableColumns) => Task.CompletedTask;


        public Task CreateDeleteAsync() => Task.CompletedTask;


        public Task CreateDeleteMetadataAsync() => Task.CompletedTask;


        public Task CreateResetAsync()  => Task.CompletedTask;


        public Task CreateSelectIncrementalChangesAsync(SyncFilter filter)  => Task.CompletedTask;


        public Task CreateSelectInitializedChangesAsync(SyncFilter filter)  => Task.CompletedTask;


        public Task CreateSelectRowAsync()  => Task.CompletedTask;


        public Task CreateTVPTypeAsync()  => Task.CompletedTask;


        public Task CreateUpdateAsync(bool hasMutableColumns)  => Task.CompletedTask;


        public Task DropBulkDeleteAsync()  => Task.CompletedTask;


        public Task DropBulkUpdateAsync()  => Task.CompletedTask;


        public Task DropDeleteAsync()  => Task.CompletedTask;


        public Task DropDeleteMetadataAsync()  => Task.CompletedTask;


        public Task DropResetAsync()  => Task.CompletedTask;


        public Task DropSelectIncrementalChangesAsync(SyncFilter filter)  => Task.CompletedTask;


        public Task DropSelectInitializedChangesAsync(SyncFilter filter)  => Task.CompletedTask;


        public Task DropSelectRowAsync()  => Task.CompletedTask;


        public Task DropTVPTypeAsync()  => Task.CompletedTask;


        public Task DropUpdateAsync()  => Task.CompletedTask;


        public Task<bool> NeedToCreateProcedureAsync(DbCommandType commandName) => Task.FromResult(true);


        public Task<bool> NeedToCreateTypeAsync(DbCommandType typeName) => Task.FromResult(true);

    }
}
