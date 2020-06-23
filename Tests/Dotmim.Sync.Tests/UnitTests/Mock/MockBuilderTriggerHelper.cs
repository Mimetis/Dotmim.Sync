using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class MockBuilderTriggerHelper : IDbBuilderTriggerHelper
    {
        public Task AlterDeleteTriggerAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;

        public Task AlterInsertTriggerAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;

        public Task AlterUpdateTriggerAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;

        public Task CreateDeleteTriggerAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;

        public Task CreateInsertTriggerAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;

        public Task CreateUpdateTriggerAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;

        public Task DropDeleteTriggerAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;

        public Task DropInsertTriggerAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;

        public Task DropUpdateTriggerAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;

        public Task<bool> NeedToCreateTriggerAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction) => Task.FromResult(true);
    }
}
