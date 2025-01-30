using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
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
        public async Task Trigger_Create_One()
        {
            var localOrchestrator = new LocalOrchestrator(clientProvider, options);
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var setup = new SyncSetup("SalesLT.Product");

            // 1) create a console logger
            //var loggerFactory = LoggerFactory.Create(builder => { builder.AddDebug().SetMinimumLevel(LogLevel.Debug); });
            //var logger = loggerFactory.CreateLogger("Dotmim.Sync");
            var logger = new SyncLogger().AddDebug().SetMinimumLevel(LogLevel.Debug);
            options.Logger = logger;


            var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            remoteOrchestrator.OnTriggerCreating(tca => onCreating++);
            remoteOrchestrator.OnTriggerCreated(tca => onCreated++);
            remoteOrchestrator.OnTriggerDropping(tca => onDropping++);
            remoteOrchestrator.OnTriggerDropped(tca => onDropped++);

            var isCreated = await remoteOrchestrator.CreateTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert);

            Assert.True(isCreated);
            Assert.Equal(1, onCreating);
            Assert.Equal(1, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            // Check 
            await using (var c = new SqlConnection(serverProvider.ConnectionString))
            {
                await c.OpenAsync();
                var check = await SqlManagementUtils.GetTriggerAsync("Product_insert_trigger", "SalesLT", c, null);
                Assert.Single(check.Rows);
                c.Close();
            }

        }

        [Fact]
        public async Task Trigger_Exists()
        {
            var localOrchestrator = new LocalOrchestrator(clientProvider, options);
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var setup = new SyncSetup("SalesLT.Product");

            var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);

            await remoteOrchestrator.CreateTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert);

            var exists = await remoteOrchestrator.ExistTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert);
            Assert.True(exists);

            exists = await remoteOrchestrator.ExistTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Update);
            Assert.False(exists);

        }

        [Fact]
        public async Task Trigger_Create_All()
        {
            var localOrchestrator = new LocalOrchestrator(clientProvider, options);
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var setup = new SyncSetup("SalesLT.Product");

            var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            remoteOrchestrator.OnTriggerCreating(tca => onCreating++);
            remoteOrchestrator.OnTriggerCreated(tca => onCreated++);
            remoteOrchestrator.OnTriggerDropping(tca => onDropping++);
            remoteOrchestrator.OnTriggerDropped(tca => onDropped++);

            var isCreated = await remoteOrchestrator.CreateTriggersAsync(scopeInfo, "Product", "SalesLT");

            Assert.True(isCreated);
            Assert.Equal(3, onCreating);
            Assert.Equal(3, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            // Check 
            await using (var c = new SqlConnection(serverProvider.ConnectionString))
            {
                await c.OpenAsync();
                var check = await SqlManagementUtils.GetTriggerAsync("Product_insert_trigger", "SalesLT", c, null);
                Assert.Single(check.Rows);
                check = await SqlManagementUtils.GetTriggerAsync("Product_update_trigger", "SalesLT", c, null);
                Assert.Single(check.Rows);
                check = await SqlManagementUtils.GetTriggerAsync("Product_delete_trigger", "SalesLT", c, null);
                Assert.Single(check.Rows);
                c.Close();
            }
        }

        [Fact]
        public async Task Trigger_Drop_One()
        {
            var localOrchestrator = new LocalOrchestrator(clientProvider, options);
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var setup = new SyncSetup("SalesLT.Product");

            var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);

            var isCreated = await remoteOrchestrator.CreateTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert);
            Assert.True(isCreated);

            // Ensuring we have a clean new instance
            remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            remoteOrchestrator.OnTriggerCreating(tca => onCreating++);
            remoteOrchestrator.OnTriggerCreated(tca => onCreated++);
            remoteOrchestrator.OnTriggerDropping(tca => onDropping++);
            remoteOrchestrator.OnTriggerDropped(tca => onDropped++);

            var isDropped = await remoteOrchestrator.DropTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert);

            Assert.True(isDropped);
            Assert.Equal(0, onCreating);
            Assert.Equal(0, onCreated);
            Assert.Equal(1, onDropping);
            Assert.Equal(1, onDropped);

            // Check 
            await using (var c = new SqlConnection(serverProvider.ConnectionString))
            {
                await c.OpenAsync();
                var check = await SqlManagementUtils.GetTriggerAsync("Product_insert_trigger", "SalesLT", c, null);
                Assert.Empty(check.Rows);
                c.Close();
            }

            // try to delete a non existing one
            isDropped = await remoteOrchestrator.DropTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Update);

            Assert.False(isDropped);
        }

        [Fact]
        public async Task Trigger_Drop_One_Cancel()
        {
            var localOrchestrator = new LocalOrchestrator(clientProvider, options);
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var setup = new SyncSetup("SalesLT.Product");

            var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);

            var isCreated = await remoteOrchestrator.CreateTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert);
            Assert.True(isCreated);

            // Ensuring we have a clean new instance
            remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            remoteOrchestrator.OnTriggerCreating(tca => onCreating++);

            remoteOrchestrator.OnTriggerCreated(tca => onCreated++);
            remoteOrchestrator.OnTriggerDropping(tca =>
            {
                tca.Cancel = true;
                onDropping++;
            });
            remoteOrchestrator.OnTriggerDropped(tca => onDropped++);

            var isDropped = await remoteOrchestrator.DropTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert);

            Assert.False(isDropped);
            Assert.Equal(0, onCreating);
            Assert.Equal(0, onCreated);
            Assert.Equal(1, onDropping);
            Assert.Equal(0, onDropped);

            // Check 
            await using (var c = new SqlConnection(serverProvider.ConnectionString))
            {
                await c.OpenAsync();
                var check = await SqlManagementUtils.GetTriggerAsync("Product_insert_trigger", "SalesLT", c, null);
                Assert.Single(check.Rows);
                c.Close();
            }
        }

        [Fact]
        public async Task Trigger_Create_One_Overwrite()
        {
            var localOrchestrator = new LocalOrchestrator(clientProvider, options);
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var setup = new SyncSetup("SalesLT.Product");

            var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            remoteOrchestrator.OnTriggerCreating(tca => onCreating++);
            remoteOrchestrator.OnTriggerCreated(tca => onCreated++);
            remoteOrchestrator.OnTriggerDropping(tca => onDropping++);
            remoteOrchestrator.OnTriggerDropped(tca => onDropped++);

            var isCreated = await remoteOrchestrator.CreateTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert);

            Assert.True(isCreated);
            Assert.Equal(1, onCreating);
            Assert.Equal(1, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            onCreating = 0;
            onCreated = 0;
            onDropping = 0;
            onDropped = 0;

            isCreated = await remoteOrchestrator.CreateTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert);

            Assert.False(isCreated);
            Assert.Equal(0, onCreating);
            Assert.Equal(0, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);

            isCreated = await remoteOrchestrator.CreateTriggerAsync(scopeInfo, "Product", "SalesLT", DbTriggerType.Insert, true);

            Assert.True(isCreated);
            Assert.Equal(1, onCreating);
            Assert.Equal(1, onCreated);
            Assert.Equal(1, onDropping);
            Assert.Equal(1, onDropped);
        }

        [Fact]
        public async Task Trigger_Drop_All()
        {
            var localOrchestrator = new LocalOrchestrator(clientProvider, options);
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            var setup = new SyncSetup("SalesLT.Product");

            var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);

            var onCreating = 0;
            var onCreated = 0;
            var onDropping = 0;
            var onDropped = 0;

            remoteOrchestrator.OnTriggerCreating(tca => onCreating++);
            remoteOrchestrator.OnTriggerCreated(tca => onCreated++);
            remoteOrchestrator.OnTriggerDropping(tca => onDropping++);
            remoteOrchestrator.OnTriggerDropped(tca => onDropped++);

            var isCreated = await remoteOrchestrator.CreateTriggersAsync(scopeInfo, "Product", "SalesLT");

            Assert.True(isCreated);
            Assert.Equal(3, onCreating);
            Assert.Equal(3, onCreated);
            Assert.Equal(0, onDropping);
            Assert.Equal(0, onDropped);


            onCreating = 0;
            onCreated = 0;
            onDropping = 0;
            onDropped = 0;

            var isDropped = await remoteOrchestrator.DropTriggersAsync(scopeInfo, "Product", "SalesLT");

            Assert.True(isCreated);
            Assert.Equal(0, onCreating);
            Assert.Equal(0, onCreated);
            Assert.Equal(3, onDropping);
            Assert.Equal(3, onDropped);
        }

    }
}
