using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class MockBuilderTrackingTableHelper : IDbBuilderTrackingTableHelper
    {
        public Task CreateIndexAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;


        public Task CreatePkAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;


        public Task CreateTableAsync(DbConnection connection, DbTransaction transaction)  => Task.CompletedTask;


        public Task DropTableAsync(DbConnection connection, DbTransaction transaction)  => Task.CompletedTask;


        public Task<bool> NeedToCreateTrackingTableAsync(DbConnection connection, DbTransaction transaction) => Task.FromResult(true);

        
        public Task RenameTableAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction) => Task.CompletedTask;
    }
}
