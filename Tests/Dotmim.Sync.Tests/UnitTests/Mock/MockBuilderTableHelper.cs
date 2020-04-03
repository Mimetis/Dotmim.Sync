using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class MockBuilderTableHelper : IDbBuilderTableHelper
    {
        public Task CreateForeignKeyConstraintsAsync(SyncRelation constraint)  => Task.CompletedTask;


        public Task CreatePrimaryKeyAsync()  => Task.CompletedTask;


        public Task CreateSchemaAsync()  => Task.CompletedTask;


        public Task CreateTableAsync()  => Task.CompletedTask;


        public Task DropTableAsync()  => Task.CompletedTask;


        public Task<bool> NeedToCreateForeignKeyConstraintsAsync(SyncRelation constraint) => Task.FromResult(true);

        public Task<bool> NeedToCreateSchemaAsync() => Task.FromResult(true);

        public Task<bool> NeedToCreateTableAsync() => Task.FromResult(true);
    }
}
