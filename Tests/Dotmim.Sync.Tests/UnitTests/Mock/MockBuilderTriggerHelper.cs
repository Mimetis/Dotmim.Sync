using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class MockBuilderTriggerHelper : IDbBuilderTriggerHelper
    {
        public Task AlterDeleteTriggerAsync() => Task.CompletedTask;

        public Task AlterInsertTriggerAsync() => Task.CompletedTask;

        public Task AlterUpdateTriggerAsync() => Task.CompletedTask;

        public Task CreateDeleteTriggerAsync() => Task.CompletedTask;

        public Task CreateInsertTriggerAsync() => Task.CompletedTask;

        public Task CreateUpdateTriggerAsync() => Task.CompletedTask;

        public Task DropDeleteTriggerAsync() => Task.CompletedTask;

        public Task DropInsertTriggerAsync() => Task.CompletedTask;

        public Task DropUpdateTriggerAsync() => Task.CompletedTask;

        public Task<bool> NeedToCreateTriggerAsync(DbTriggerType triggerType) => Task.FromResult(true);
    }
}
