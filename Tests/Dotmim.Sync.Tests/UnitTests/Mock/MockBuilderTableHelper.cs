using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class MockBuilderTableHelper : IDbBuilderTableHelper
    {
        public Task CreateForeignKeyConstraintsAsync(SyncRelation constraint, DbConnection connection, DbTransaction transaction)  => Task.CompletedTask;


        public Task CreatePrimaryKeyAsync(DbConnection connection, DbTransaction transaction)  => Task.CompletedTask;


        public Task CreateSchemaAsync(DbConnection connection, DbTransaction transaction)  => Task.CompletedTask;


        public Task CreateTableAsync(DbConnection connection, DbTransaction transaction)  => Task.CompletedTask;


        public Task DropTableAsync(DbConnection connection, DbTransaction transaction)  => Task.CompletedTask;


        public Task<bool> NeedToCreateForeignKeyConstraintsAsync(SyncRelation constraint, DbConnection connection, DbTransaction transaction) => Task.FromResult(true);

        public Task<bool> NeedToCreateSchemaAsync(DbConnection connection, DbTransaction transaction) => Task.FromResult(true);

        public Task<bool> NeedToCreateTableAsync(DbConnection connection, DbTransaction transaction) => Task.FromResult(true);
    }
}
