using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class MockBuilderTrackingTableHelper : IDbBuilderTrackingTableHelper
    {
        public Task CreateIndexAsync() => Task.CompletedTask;


        public Task CreatePkAsync() => Task.CompletedTask;


        public Task CreateTableAsync()  => Task.CompletedTask;


        public Task DropTableAsync()  => Task.CompletedTask;


        public Task<bool> NeedToCreateTrackingTableAsync() => Task.FromResult(true);

        
        public Task RenameTableAsync(ParserName oldTableName) => Task.CompletedTask;
    }
}
