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
    public partial class InterceptorsTests
    {
        [Fact]
        public async Task StoredProcedure_Create_One()
        {
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);
            var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            remoteOrchestrator.OnStoredProcedureCreating(tca => onCreating++);
            remoteOrchestrator.OnStoredProcedureCreated(tca => onCreated++);
            remoteOrchestrator.OnStoredProcedureDropping(tca => onDropping++);
            remoteOrchestrator.OnStoredProcedureDropped(tca => onDropped++);

            var isCreated = await remoteOrchestrator.CreateStoredProcedureAsync(scopeInfo,
                "Product", "SalesLT", DbStoredProcedureType.SelectChanges);

            Assert.True(isCreated);
            Assert.Equal(1, onCreating);
            Assert.Equal(1, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            // Check 
            await using (var c = new SqlConnection(serverProvider.ConnectionString))
            {
                await c.OpenAsync();
                var check = await SqlManagementUtils.ProcedureExistsAsync(c, null, "SalesLT.Product_changes");
                Assert.True(check);
                c.Close();
            }
        }

        [Fact]
        public async Task StoredProcedure_Exists()
        {
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);

            await remoteOrchestrator.CreateStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges);

            var exists = await remoteOrchestrator.ExistStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges);
            Assert.True(exists);

            exists = await remoteOrchestrator.ExistStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChangesWithFilters);
            Assert.False(exists);
        }


        [Fact]
        public async Task StoredProcedure_Create_All()
        {
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            remoteOrchestrator.OnStoredProcedureCreating(tca => onCreating++);
            remoteOrchestrator.OnStoredProcedureCreated(tca => onCreated++);
            remoteOrchestrator.OnStoredProcedureDropping(tca => onDropping++);
            remoteOrchestrator.OnStoredProcedureDropped(tca => onDropped++);

            var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);

            var isCreated = await remoteOrchestrator.CreateStoredProceduresAsync(scopeInfo, "Product", "SalesLT");

            Assert.True(isCreated);
            Assert.Equal(7, onCreating);
            Assert.Equal(7, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            // Check 
            await using (var c = new SqlConnection(serverProvider.ConnectionString))
            {
                await c.OpenAsync();
                var check = await SqlManagementUtils.ProcedureExistsAsync(c, null, "SalesLT.Product_bulkdelete");
                Assert.True(check);
                check = await SqlManagementUtils.ProcedureExistsAsync(c, null, "SalesLT.Product_bulkupdate");
                Assert.True(check);
                check = await SqlManagementUtils.ProcedureExistsAsync(c, null, "SalesLT.Product_changes");
                Assert.True(check);
                check = await SqlManagementUtils.ProcedureExistsAsync(c, null, "SalesLT.Product_delete");
                Assert.True(check);
                check = await SqlManagementUtils.ProcedureExistsAsync(c, null, "SalesLT.Product_initialize");
                Assert.True(check);
                check = await SqlManagementUtils.ProcedureExistsAsync(c, null, "SalesLT.Product_update");
                Assert.True(check);
                c.Close();
            }
        }

        [Fact]
        public async Task StoredProcedure_Create_All_Overwrite()
        {
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);
            var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            remoteOrchestrator.OnStoredProcedureCreating(tca => onCreating++);
            remoteOrchestrator.OnStoredProcedureCreated(tca => onCreated++);
            remoteOrchestrator.OnStoredProcedureDropping(tca => onDropping++);
            remoteOrchestrator.OnStoredProcedureDropped(tca => onDropped++);

            var isCreated = await remoteOrchestrator.CreateStoredProceduresAsync(scopeInfo, "Product", "SalesLT");

            Assert.True(isCreated);
            Assert.Equal(7, onCreating);
            Assert.Equal(7, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            onCreating = 0;
            onCreated = 0;
            onDropping = 0;
            onDropped = 0;

            isCreated = await remoteOrchestrator.CreateStoredProceduresAsync(scopeInfo, "Product", "SalesLT");

            Assert.False(isCreated);
            Assert.Equal(0, onCreating);
            Assert.Equal(0, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            onCreating = 0;
            onCreated = 0;
            onDropping = 0;
            onDropped = 0;

            isCreated = await remoteOrchestrator.CreateStoredProceduresAsync(scopeInfo, "Product", "SalesLT", true);

            Assert.True(isCreated);
            Assert.Equal(7, onCreating);
            Assert.Equal(7, onCreated);
            Assert.Equal(7, onDropping);
            Assert.Equal(7, onDropped);
        }



        [Fact]
        public async Task StoredProcedure_Drop_One()
        {
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);
            var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);

            var isCreated = await remoteOrchestrator.CreateStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges);
            Assert.True(isCreated);

            // Ensuring we have a clean new instance
            remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            remoteOrchestrator.OnStoredProcedureCreating(tca => onCreating++);
            remoteOrchestrator.OnStoredProcedureCreated(tca => onCreated++);
            remoteOrchestrator.OnStoredProcedureDropping(tca => onDropping++);
            remoteOrchestrator.OnStoredProcedureDropped(tca => onDropped++);

            var isDropped = await remoteOrchestrator.DropStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges);

            Assert.True(isDropped);
            Assert.Equal(0, onCreating);
            Assert.Equal(0, onCreated);
            Assert.Equal(1, onDropping);
            Assert.Equal(1, onDropped);

            // Check 
            await using (var c = new SqlConnection(serverProvider.ConnectionString))
            {
                await c.OpenAsync();
                var check = await SqlManagementUtils.ProcedureExistsAsync(c, null, "SalesLT.Product_changes");
                Assert.False(check);
                c.Close();
            }

            // try to delete again a non existing sp type
            isDropped = await remoteOrchestrator.DropStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChangesWithFilters);

            Assert.False(isDropped);
        }

        [Fact]
        public async Task StoredProcedure_Drop_One_Cancel()
        {
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);

            var isCreated = await remoteOrchestrator.CreateStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges);
            Assert.True(isCreated);

            // Ensuring we have a clean new instance
            remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            remoteOrchestrator.OnStoredProcedureCreating(tca => onCreating++);

            remoteOrchestrator.OnStoredProcedureCreated(tca => onCreated++);
            remoteOrchestrator.OnStoredProcedureDropping(tca =>
            {
                tca.Cancel = true;
                onDropping++;
            });
            remoteOrchestrator.OnStoredProcedureDropped(tca => onDropped++);

            var isDropped = await remoteOrchestrator.DropStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges);

            Assert.False(isDropped);
            Assert.Equal(0, onCreating);
            Assert.Equal(0, onCreated);
            Assert.Equal(1, onDropping);
            Assert.Equal(0, onDropped);

            // Check 
            await using (var c = new SqlConnection(serverProvider.ConnectionString))
            {
                await c.OpenAsync();
                var check = await SqlManagementUtils.ProcedureExistsAsync(c, null, "SalesLT.Product_changes");
                Assert.True(check);
                c.Close();
            }
        }

        [Fact]
        public async Task StoredProcedure_Create_One_Overwrite()
        {
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            remoteOrchestrator.OnStoredProcedureCreating(tca => onCreating++);
            remoteOrchestrator.OnStoredProcedureCreated(tca => onCreated++);
            remoteOrchestrator.OnStoredProcedureDropping(tca => onDropping++);
            remoteOrchestrator.OnStoredProcedureDropped(tca => onDropped++);

            var isCreated = await remoteOrchestrator.CreateStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges);

            Assert.True(isCreated);
            Assert.Equal(1, onCreating);
            Assert.Equal(1, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            onCreating = 0;
            onCreated = 0;
            onDropping = 0;
            onDropped = 0;

            isCreated = await remoteOrchestrator.CreateStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges);

            Assert.False(isCreated);
            Assert.Equal(0, onCreating);
            Assert.Equal(0, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            isCreated = await remoteOrchestrator.CreateStoredProcedureAsync(scopeInfo, "Product", "SalesLT", DbStoredProcedureType.SelectChanges, true);

            Assert.True(isCreated);
            Assert.Equal(1, onCreating);
            Assert.Equal(1, onCreated);
            Assert.Equal(1, onDropping);
            Assert.Equal(1, onDropped);
        }

        [Fact]
        public async Task StoredProcedure_Drop_All()
        {
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            remoteOrchestrator.OnStoredProcedureCreating(tca => onCreating++);
            remoteOrchestrator.OnStoredProcedureCreated(tca => onCreated++);
            remoteOrchestrator.OnStoredProcedureDropping(tca => onDropping++);
            remoteOrchestrator.OnStoredProcedureDropped(tca => onDropped++);

            var isCreated = await remoteOrchestrator.CreateStoredProceduresAsync(scopeInfo, "Product", "SalesLT");

            Assert.True(isCreated);
            Assert.Equal(7, onCreating);
            Assert.Equal(7, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);


            onCreating = 0;
            onCreated = 0;
            onDropping = 0;
            onDropped = 0;

            var isDropped = await remoteOrchestrator.DropStoredProceduresAsync(scopeInfo, "Product", "SalesLT");

            Assert.True(isCreated);
            Assert.Equal(0, onCreating);
            Assert.Equal(0, onCreated);
            Assert.Equal(7, onDropping);
            Assert.Equal(7, onDropped);
        }

    }
}
