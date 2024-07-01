using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests.UnitTests
{
    public partial class RemoteOrchestratorTests
    {

        [Fact]
        public async Task RemoteOrchestrator_StoredProcedure_ShouldCreate()
        {
            var scopeName = "scope";
            var setup = new SyncSetup("SalesLT.Product")
            {
                StoredProceduresPrefix = "sp_",
                StoredProceduresSuffix = "_sp"
            };

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(scopeName, setup);

            await remoteOrchestrator.CreateStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges, false);

            Assert.True(await remoteOrchestrator.ExistStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges));

            // Adding a filter to check if stored procedures "with filters" are also generated
            setup.Filters.Add("Product", "ProductCategoryID", "SalesLT");

            // Create a new scope with this filter
            var scopeName2 = "scope2";
            var scopeInfo2 = await remoteOrchestrator.GetScopeInfoAsync(scopeName2, setup);

            await remoteOrchestrator.CreateStoredProcedureAsync(scopeInfo2, "Product", "SalesLT", DbStoredProcedureType.SelectChangesWithFilters, false);

            Assert.True(await remoteOrchestrator.ExistStoredProcedureAsync(scopeInfo2, "Product", "SalesLT", DbStoredProcedureType.SelectChangesWithFilters));
        }


        [Fact]
        public async Task RemoteOrchestrator_StoredProcedure_ShouldOverwrite()
        {
            var scopeName = "scope";
            var setup = new SyncSetup("SalesLT.Product")
            {
                StoredProceduresPrefix = "sp_",
                StoredProceduresSuffix = "_sp"
            };

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(scopeName, setup);

            var storedProcedureSelectChanges = $"SalesLT.{setup.StoredProceduresPrefix}Product{setup.StoredProceduresSuffix}_changes";

            await remoteOrchestrator.CreateStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges, false);

            var assertOverWritten = false;

            remoteOrchestrator.OnStoredProcedureCreating(args =>
            {
                assertOverWritten = true;
            });

            await remoteOrchestrator.CreateStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges, true);

            Assert.True(assertOverWritten);
        }


        [Fact]
        public async Task RemoteOrchestrator_StoredProcedure_ShouldNotOverwrite()
        {
            var scopeName = "scope";

            var options = new SyncOptions();
            var setup = new SyncSetup("SalesLT.Product")
            {
                StoredProceduresPrefix = "sp_",
                StoredProceduresSuffix = "_sp"
            };

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(scopeName, setup);

            var storedProcedureSelectChanges = $"SalesLT.{setup.StoredProceduresPrefix}Product{setup.StoredProceduresSuffix}_changes";

            await remoteOrchestrator.CreateStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges, false);

            var assertOverWritten = false;

            remoteOrchestrator.OnStoredProcedureCreating(args =>
            {
                assertOverWritten = true;
            });

            await remoteOrchestrator.CreateStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges, false);

            Assert.False(assertOverWritten);
        }

        [Fact]
        public async Task RemoteOrchestrator_StoredProcedure_Exists()
        {
            var scopeName = "scope";

            var options = new SyncOptions();
            var setup = new SyncSetup("SalesLT.Product")
            {
                StoredProceduresPrefix = "sp_",
                StoredProceduresSuffix = "_sp"
            };

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(scopeName, setup);

            await remoteOrchestrator.CreateStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges, false);

            Assert.True(await remoteOrchestrator.ExistStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges));
            Assert.False(await remoteOrchestrator.ExistStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.UpdateRow));
        }

        [Fact]
        public async Task RemoteOrchestrator_StoredProcedures_ShouldCreate()
        {
            var scopeName = "scope";

            var options = new SyncOptions();
            var setup = new SyncSetup("SalesLT.Product")
            {
                StoredProceduresPrefix = "sp_",
                StoredProceduresSuffix = "_sp"
            };
            // Adding a filter to check if stored procedures "with filters" are also generated
            setup.Filters.Add("Product", "ProductCategoryID", "SalesLT");

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(scopeName, setup);

            await remoteOrchestrator.CreateStoredProceduresAsync(scopeInfo, "Product", "SalesLT");

            Assert.True(await remoteOrchestrator.ExistStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.BulkDeleteRows));
            Assert.True(await remoteOrchestrator.ExistStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.BulkTableType));
            Assert.True(await remoteOrchestrator.ExistStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.BulkUpdateRows));
            Assert.True(await remoteOrchestrator.ExistStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.DeleteRow));
            Assert.True(await remoteOrchestrator.ExistStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges));
            Assert.True(await remoteOrchestrator.ExistStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectInitializedChanges));
            Assert.True(await remoteOrchestrator.ExistStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.UpdateRow));
            Assert.True(await remoteOrchestrator.ExistStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChangesWithFilters));
            Assert.True(await remoteOrchestrator.ExistStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectInitializedChangesWithFilters));
        }
    }
}
