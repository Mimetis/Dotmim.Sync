using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SampleConsole;
using Dotmim.Sync.Serialization;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Tests.Models;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using MySqlConnector;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Builder;
using System.Net.Http.Headers;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Tokens;
using System.IdentityModel.Tokens.Jwt;

namespace Dotmim.Sync.Tests
{
    public abstract partial class HttpTests : HttpTestsBase
    {
        protected HttpTests(HelperProvider fixture, ITestOutputHelper output) : base(fixture, output)
        {
        }

        [Fact]
        public virtual async Task SchemaIsCreated()
        {
            // create a server db without seed
            await this.EnsureDatabaseSchemaAndSeedAsync(Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName, new SyncSetup(Tables));

            var serviceUri = this.Kestrell.Run();

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public virtual async Task RowsCount(SyncOptions options)
        {
            // create a server db and seed it
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName, new SyncSetup(Tables));
            var serviceUri = this.Kestrell.Run();

            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
            }
        }

        /// <summary>
        /// Check a bad connection should raise correct error
        /// </summary>
        [Fact]
        public async Task Bad_ConnectionFromServer_ShouldRaiseError()
        {

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            var onReconnect = new Action<ReConnectArgs>(args =>
                 Console.WriteLine($"Can't connect to database {args.Connection?.Database}. Retry N°{args.Retry}. Waiting {args.WaitingTimeSpan.Milliseconds}. Exception:{args.HandledException.Message}."));

            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), $@"Server=unknown;Database=unknown;UID=sa;PWD=unknown",
                SyncOptions.DefaultScopeName, new SyncSetup(Tables));

            var serviceUri = this.Kestrell.Run();


            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);
                webRemoteOrchestrator.SyncPolicy.RetryCount = 0;

                var agent = new SyncAgent(client.Provider, webRemoteOrchestrator);

                agent.LocalOrchestrator.OnReConnect(onReconnect);

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync();

                });
            }
        }

        [Fact]
        public async Task Bad_TableWithoutPrimaryKeys_ShouldRaiseError()
        {
            string tableTestCreationScript = "Create Table TableTest (TestId int, TestName varchar(50))";

            // Create an empty server database
            await this.CreateDatabaseAsync(this.ServerType, this.Server.DatabaseName, true);

            // Create the table on the server
            await HelperDatabase.ExecuteScriptAsync(this.Server.ProviderType, this.Server.DatabaseName, tableTestCreationScript);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                new SyncSetup("TableTest"));

            var serviceUri = this.Kestrell.Run();

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);
                webRemoteOrchestrator.SyncPolicy.RetryCount = 0;
                var agent = new SyncAgent(client.Provider, webRemoteOrchestrator);

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync();
                });

                Assert.Equal(SyncSide.ServerSide, se.Side);
                Assert.Equal("MissingPrimaryKeyException", se.TypeName);
                Assert.Equal(this.Server.DatabaseName, se.InitialCatalog);

            }
        }

        [Fact]
        public async Task Bad_ColumnSetup_DoesNotExistInSchema_ShouldRaiseError()
        {
            // create a server db without seed
            await this.EnsureDatabaseSchemaAndSeedAsync(Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);


            // Add a malformatted column name
            var setup = new SyncSetup(Tables);
            setup.Tables["Employee"].Columns.AddRange(new string[] { "EmployeeID", "FirstName", "LastNam" });

            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString,
                SyncOptions.DefaultScopeName, setup);

            var serviceUri = this.Kestrell.Run();

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);
                webRemoteOrchestrator.SyncPolicy.RetryCount = 0;

                var agent = new SyncAgent(client.Provider, webRemoteOrchestrator);

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync();
                });

                Assert.Equal(SyncSide.ServerSide, se.Side);
                Assert.Equal("MissingColumnException", se.TypeName);
            }
        }

        [Fact]
        public async Task Bad_TableSetup_DoesNotExistInSchema_ShouldRaiseError()
        {
            // create a server db without seed
            await this.EnsureDatabaseSchemaAndSeedAsync(Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Add a malformatted column name
            var setup = new SyncSetup("WeirdTable");

            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString,
                SyncOptions.DefaultScopeName, setup);

            var serviceUri = this.Kestrell.Run();

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);
                webRemoteOrchestrator.SyncPolicy.RetryCount = 0;

                var agent = new SyncAgent(client.Provider, webRemoteOrchestrator);

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync();
                });

                Assert.Equal(SyncSide.ServerSide, se.Side);
                Assert.Equal("MissingTableException", se.TypeName);
            }
        }

        /// <summary>
        /// Insert one row on server, should be correctly sync on all clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Insert_OneTable_FromServer(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                new SyncSetup(Tables));

            var serviceUri = this.Kestrell.Run();

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Create a new product on server
            var name = HelperDatabase.GetRandomName();
            var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

            var product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber };

            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                serverDbCtx.Product.Add(product);
                await serverDbCtx.SaveChangesAsync();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }
        }

        /// <summary>
        /// Insert one row on each client, should be sync on server and clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Insert_OneTable_FromClient(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                new SyncSetup(Tables));

            var serviceUri = this.Kestrell.Run();

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Insert one line on each client
            foreach (var client in Clients)
            {
                var name = HelperDatabase.GetRandomName();
                var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber };

                using var serverDbCtx = new AdventureWorksContext(client, this.UseFallbackSchema);
                serverDbCtx.Product.Add(product);
                await serverDbCtx.SaveChangesAsync();
            }

            // Sync all clients
            // First client  will upload one line and will download nothing
            // Second client will upload one line and will download one line
            // thrid client  will upload one line and will download two lines
            int download = 0;
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);
                var s = await agent.SynchronizeAsync();

                Assert.Equal(download++, s.TotalChangesDownloaded);
                Assert.Equal(1, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

        }

        /// <summary>
        /// Delete rows on server, should be correctly sync on all clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Delete_OneTable_FromServer(SyncOptions options)
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // get rows count
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                new SyncSetup(Tables));

            var serviceUri = this.Kestrell.Run();

            // part of the filter
            var employeeId = 1;
            // will be defined when address is inserted
            var addressId = 0;

            // Insert one address row and one addressemployee row
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                // Insert a new address for employee 1
                var city = "Paris " + HelperDatabase.GetRandomName();
                var addressline1 = "Rue Monthieu " + HelperDatabase.GetRandomName();
                var stateProvince = "Ile de France";
                var countryRegion = "France";
                var postalCode = "75001";

                var address = new Address
                {
                    AddressLine1 = addressline1,
                    City = city,
                    StateProvince = stateProvince,
                    CountryRegion = countryRegion,
                    PostalCode = postalCode

                };

                serverDbCtx.Add(address);
                await serverDbCtx.SaveChangesAsync();
                addressId = address.AddressId;

                var employeeAddress = new EmployeeAddress
                {
                    EmployeeId = employeeId,
                    AddressId = address.AddressId,
                    AddressType = "SERVER"
                };

                var ea = serverDbCtx.EmployeeAddress.Add(employeeAddress);
                await serverDbCtx.SaveChangesAsync();

            }

            // add 2 lines to rows count
            rowsCount += 2;

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

                // check rows are create on client
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var finalAddressesCount = await ctx.Address.AsNoTracking().CountAsync(a => a.AddressId == addressId);
                var finalEmployeeAddressesCount = await ctx.EmployeeAddress.AsNoTracking().CountAsync(a => a.AddressId == addressId && a.EmployeeId == employeeId);
                Assert.Equal(1, finalAddressesCount);
                Assert.Equal(1, finalEmployeeAddressesCount);


            }

            // Delete those lines from server
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                // Get the addresses query
                var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == addressId);
                var empAddress = await serverDbCtx.EmployeeAddress.SingleAsync(a => a.AddressId == addressId && a.EmployeeId == employeeId);

                // remove them
                serverDbCtx.EmployeeAddress.Remove(empAddress);
                serverDbCtx.Address.Remove(address);

                // Execute query
                await serverDbCtx.SaveChangesAsync();
            }

            // Sync and check we have delete these lines on each server
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

                // check row deleted on client values
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var finalAddressesCount = await ctx.Address.AsNoTracking().CountAsync(a => a.AddressId == addressId);
                var finalEmployeeAddressesCount = await ctx.EmployeeAddress.AsNoTracking().CountAsync(a => a.AddressId == addressId && a.EmployeeId == employeeId);
                Assert.Equal(0, finalAddressesCount);
                Assert.Equal(0, finalEmployeeAddressesCount);
            }
        }

        /// <summary>
        /// Insert thousand or rows. Check if batch mode works correctly
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Insert_ThousandRows_FromClient(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                new SyncSetup(Tables));

            var serviceUri = this.Kestrell.Run();

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);
                await agent.SynchronizeAsync();
            }

            // Insert one thousand lines on each client
            foreach (var client in Clients)
            {
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                for (var i = 0; i < 2000; i++)
                {
                    var name = HelperDatabase.GetRandomName();
                    var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                    var product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber };

                    ctx.Product.Add(product);
                }
                await ctx.SaveChangesAsync();
            }

            // Sync all clients
            // First client  will upload 2000 lines and will download nothing
            // Second client will upload 2000 lines and will download 2000 lines
            // Third client  will upload 2000 line and will download 4000 lines
            int download = 0;
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);
                var s = await agent.SynchronizeAsync();

                Assert.Equal(download * 2000, s.TotalChangesDownloaded);
                Assert.Equal(2000, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

                download++;
            }

        }

        /// <summary>
        /// Insert one row on each client, should be sync on server and clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Reinitialize_Client(SyncOptions options)
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                new SyncSetup(Tables));

            var serviceUri = this.Kestrell.Run();

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);
                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Insert one line on each client
            foreach (var client in Clients)
            {
                var productCategoryName = HelperDatabase.GetRandomName();
                var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

                var productId = Guid.NewGuid();
                var productName = HelperDatabase.GetRandomName();
                var productNumber = productName.ToUpperInvariant().Substring(0, 10);

                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.Add(pc);
                var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber, ProductCategoryId = productCategoryId };
                ctx.Add(product);
                await ctx.SaveChangesAsync();
            }

            // Sync all clients
            // inserted rows will be deleted 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);

                var s = await agent.SynchronizeAsync(SyncType.Reinitialize);

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

            }
        }

        /// <summary>
        /// Insert one row on each client, should be sync on server and clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task ReinitializeWithUpload_Client(SyncOptions options)
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                new SyncSetup(Tables));

            var serviceUri = this.Kestrell.Run();

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);
                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Insert one line on each client
            foreach (var client in Clients)
            {
                var productCategoryName = HelperDatabase.GetRandomName();
                var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

                var productId = Guid.NewGuid();
                var productName = HelperDatabase.GetRandomName();
                var productNumber = productName.ToUpperInvariant().Substring(0, 10);

                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.Add(pc);
                var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber, ProductCategoryId = productCategoryId };
                ctx.Add(product);
                await ctx.SaveChangesAsync();
            }

            // Sync all clients
            // client  will upload two lines and will download all + its two lines
            int download = 2;
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);
                var s = await agent.SynchronizeAsync(SyncType.ReinitializeWithUpload);

                Assert.Equal(rowsCount + download, s.TotalChangesDownloaded);
                Assert.Equal(2, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
                download += 2;
            }


        }

        /// <summary>
        /// Insert one row on each client, should be sync on server and clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Bad_Converter_NotRegisteredOnServer_ShouldRaiseError(SyncOptions options)
        {
            // create a server db and seed it
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                new SyncSetup(Tables));

            var serviceUri = this.Kestrell.Run();

            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                // Add a converter on the client.
                // But this converter is not register on the server side converters list.
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri, new DateConverter());
                webRemoteOrchestrator.SyncPolicy.RetryCount = 0;

                var agent = new SyncAgent(client.Provider, webRemoteOrchestrator, options);

                var exception = await Assert.ThrowsAsync<HttpSyncWebException>(async () =>
                {
                    var s = await agent.SynchronizeAsync();

                });

                Assert.Equal("HttpConverterNotConfiguredException", exception.TypeName);
            }
        }

        /// <summary>
        /// Check web interceptors are working correctly
        /// </summary>
        //[Theory]
        //[ClassData(typeof(SyncOptionsData))]
        public async Task Check_Interceptors_WebServerOrchestrator(SyncOptions options)
        {
            // create a server db and seed it
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                new SyncSetup(Tables));

            // Create server web proxy
            var serverHandler = new RequestDelegate(async context =>
            {
                var webServerAgent = context.RequestServices.GetService(typeof(WebServerAgent)) as WebServerAgent;

                webServerAgent.OnHttpGettingRequest(r =>
                {
                    Assert.NotNull(r.HttpContext);
                    Assert.NotNull(r.Context);
                });

                webServerAgent.OnHttpSendingResponse(r =>
                {
                    Assert.NotNull(r.HttpContext);
                    Assert.NotNull(r.Context);
                });

                await webServerAgent.HandleRequestAsync(context);
            });

            var serviceUri = this.Kestrell.Run(serverHandler);

            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
            }
        }

        /// <summary>
        /// Check web interceptors are working correctly
        /// </summary>
        //[Theory]
        //[ClassData(typeof(SyncOptionsData))]
        public async Task Check_Interceptors_WebRemoteOrchestrator(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                new SyncSetup(Tables));

            var serviceUri = this.Kestrell.Run();

            foreach (var client in Clients)
            {
                var wenClientOrchestrator = new WebRemoteOrchestrator(serviceUri);
                var agent = new SyncAgent(client.Provider, wenClientOrchestrator, options);

                // Interceptor on sending scopes
                wenClientOrchestrator.OnHttpGettingScopeResponse(sra =>
                {
                    // check we a scope name
                    Assert.NotNull(sra.Context);
                });

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

                wenClientOrchestrator.ClearInterceptors();
            }

            // Insert one line on each client
            foreach (var client in Clients)
            {
                var name = HelperDatabase.GetRandomName();
                var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber };

                using var serverDbCtx = new AdventureWorksContext(client, this.UseFallbackSchema);
                serverDbCtx.Product.Add(product);
                await serverDbCtx.SaveChangesAsync();
            }

            // Sync all clients
            // First client  will upload one line and will download nothing
            // Second client will upload one line and will download one line
            // thrid client  will upload one line and will download two lines
            int download = 0;
            foreach (var client in Clients)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);
                var agent = new SyncAgent(client.Provider, webRemoteOrchestrator, options);

                // Just before sending changes, get changes sent
                webRemoteOrchestrator.OnHttpSendingChangesRequest(sra =>
                {
                    // check we have rows
                    Assert.True(sra.Request.Changes.HasRows);
                });


                var s = await agent.SynchronizeAsync();

                Assert.Equal(download++, s.TotalChangesDownloaded);
                Assert.Equal(1, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

                webRemoteOrchestrator.ClearInterceptors();
            }

        }

        /// <summary>
        /// Insert one row on each client, should be sync on server and clients
        /// </summary>
        //[Theory]
        //[ClassData(typeof(SyncOptionsData))]
        public async Task Converter_Registered_ShouldConvertDateTime(SyncOptions options)
        {
            // create a server db and seed it
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            var webServerOptions = new WebServerOptions();
            webServerOptions.Converters.Add(new DateConverter());

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                new SyncSetup(Tables), null, webServerOptions);

            // Create server web proxy
            var serverHandler = new RequestDelegate(async context =>
            {
                var webServerAgent = context.RequestServices.GetService(typeof(WebServerAgent)) as WebServerAgent;

                // Get response just before response sent back from server
                // Assert if datetime are correctly converted to long
                webServerAgent.OnHttpSendingChanges(sra =>
                {
                    if (sra.Response.Changes == null)
                        return;

                    // check we have rows
                    Assert.True(sra.Response.Changes.HasRows);

                    // getting a table where we know we have date time
                    var table = sra.Response.Changes.Tables.FirstOrDefault(t => t.TableName == "Employee");

                    if (table != null)
                    {
                        Assert.NotEmpty(table.Rows);

                        foreach (var row in table.Rows)
                        {
                            var dateCell = row[5];

                            // check we have an integer here
                            Assert.IsType<long>(dateCell);
                        }
                    }
                });


                await webServerAgent.HandleRequestAsync(context);
            });

            var serviceUri = this.Kestrell.Run(serverHandler);


            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri, new DateConverter());
                var agent = new SyncAgent(client.Provider, webRemoteOrchestrator, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
            }
        }


        /// <summary>
        /// Insert one row in two tables on server, should be correctly sync on all clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Snapshot_Initialize(SyncOptions options)
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // snapshot directory
            var snapshotDirctoryName = HelperDatabase.GetRandomName();
            var snapshotDirectory = Path.Combine(Environment.CurrentDirectory, snapshotDirctoryName);

            // configure server orchestrator
            var setup = new SyncSetup(Tables);

            var remoteOptions = new SyncOptions
            {
                SnapshotsDirectory = snapshotDirectory,
                BatchSize = 2000
            };

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                setup, remoteOptions);

            var serviceUri = this.Kestrell.Run();

            // ----------------------------------
            // Create a snapshot
            // ----------------------------------
            var remoteOrchestrator = new RemoteOrchestrator(this.Server.Provider, remoteOptions);
            await remoteOrchestrator.CreateSnapshotAsync(setup);

            // ----------------------------------
            // Add rows on server AFTER snapshot
            // ----------------------------------
            var productId = Guid.NewGuid();
            var productName = HelperDatabase.GetRandomName();
            var productNumber = productName.ToUpperInvariant().Substring(0, 10);

            var productCategoryName = HelperDatabase.GetRandomName();
            var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.Add(pc);

                var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber };
                ctx.Add(product);

                await ctx.SaveChangesAsync();
            }

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }
        }

        /// <summary>
        /// Insert one row on each client, should be sync on server and clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task IsOutdated_ShouldWork_If_Correct_Action(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);


            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                new SyncSetup(Tables));

            var serviceUri = this.Kestrell.Run();

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Call a server delete metadata to update the last valid timestamp value in scope_info_server table
            var remoteOrchestrator = new RemoteOrchestrator(this.Server.Provider);
            var dmc = await remoteOrchestrator.DeleteMetadatasAsync();

            // Insert one line on each client
            foreach (var client in Clients)
            {
                // Client side : Create a product category and a product
                var productId = Guid.NewGuid();
                var productName = HelperDatabase.GetRandomName();
                var productNumber = productName.ToUpperInvariant().Substring(0, 10);
                var productCategoryName = HelperDatabase.GetRandomName();
                var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                    ctx.Add(pc);

                    var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber };
                    ctx.Add(product);

                    await ctx.SaveChangesAsync();
                }

                // Generate an outdated situation
                await HelperDatabase.ExecuteScriptAsync(client.ProviderType, client.DatabaseName,
                                    $"Update scope_info set scope_last_server_sync_timestamp={dmc.TimestampLimit - 1}");

                // create a new agent
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);

                //// Making a first sync, will initialize everything we need
                //var se = await Assert.ThrowsAsync<SyncException>(() => agent.SynchronizeAsync());

                //Assert.Equal(SyncSide.ClientSide, se.Side);
                //Assert.Equal("OutOfDateException", se.TypeName);

                // Intercept outdated event, and make a reinitialize with upload action
                agent.LocalOrchestrator.OnOutdated(oa => oa.Action = OutdatedAction.ReinitializeWithUpload);

                var r = await agent.SynchronizeAsync();
                var c = GetServerDatabaseRowsCount(this.Server);
                Assert.Equal(c, r.TotalChangesDownloaded);
                Assert.Equal(2, r.TotalChangesUploaded);

            }


        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public virtual async Task Handling_DifferentScopeNames(SyncOptions options)
        {
            // create a server db and seed it
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, "customScope1",
                new SyncSetup(Tables));

            var serviceUri = this.Kestrell.Run();

            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);

                var s = await agent.SynchronizeAsync("customScope1");

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
            }
        }

        /// <summary>
        /// Try to get changes from server without making a first sync
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task GetChanges_BeforeServerIsInitialized(SyncOptions options)
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                new SyncSetup(Tables));

            var serviceUri = this.Kestrell.Run();

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // ----------------------------------
            // Get changes
            // ----------------------------------
            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);
                var agent = new SyncAgent(client.Provider, webRemoteOrchestrator, options);

                // Ensure scope is created locally
                var clientScope = await agent.LocalOrchestrator.GetClientScopeInfoAsync();

                // get changes from server, without any changes sent from client side
                var serverSyncChanges = await webRemoteOrchestrator.GetChangesAsync(clientScope);

                Assert.Equal(rowsCount, serverSyncChanges.ServerChangesSelected.TotalChangesSelected);
            }
        }

        /// <summary>
        /// Try to get changes from server without making a first sync
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task GetChanges_AfterServerIsInitialized(SyncOptions options)
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                new SyncSetup(Tables));

            var serviceUri = this.Kestrell.Run();

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }


            // ----------------------------------
            // Add rows on server AFTER first sync (so everything should be initialized)
            // ----------------------------------
            var productId = Guid.NewGuid();
            var productName = HelperDatabase.GetRandomName();
            var productNumber = productName.ToUpperInvariant().Substring(0, 10);

            var productCategoryName = HelperDatabase.GetRandomName();
            var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.Add(pc);

                var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber };
                ctx.Add(product);

                await ctx.SaveChangesAsync();
            }

            // ----------------------------------
            // Get changes
            // ----------------------------------
            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);
                var agent = new SyncAgent(client.Provider, webRemoteOrchestrator, options);

                // Ensure scope is created locally
                var clientScope = await agent.LocalOrchestrator.GetClientScopeInfoAsync();

                // get changes from server, without any changes sent from client side
                var serverSyncChanges = await webRemoteOrchestrator.GetChangesAsync(clientScope);

                Assert.Equal(2, serverSyncChanges.ServerChangesSelected.TotalChangesSelected);

            }
        }


        /// <summary>
        /// Try to get changes from server without making a first sync
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task GetEstimatedChangesCount_BeforeServerIsInitialized(SyncOptions options)
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                new SyncSetup(Tables));

            var serviceUri = this.Kestrell.Run();

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // ----------------------------------
            // Get changes
            // ----------------------------------
            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);
                var agent = new SyncAgent(client.Provider, webRemoteOrchestrator, options);

                // Ensure scope is created locally
                var clientScope = await agent.LocalOrchestrator.GetClientScopeInfoAsync();

                // get changes from server, without any changes sent from client side
                var changes = await webRemoteOrchestrator.GetEstimatedChangesCountAsync(clientScope);

                Assert.Equal(rowsCount, changes.ServerChangesSelected.TotalChangesSelected);
            }
        }

        /// <summary>
        /// Try to get changes from server without making a first sync
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task GetEstimatedChangesCount_AfterServerIsInitialized(SyncOptions options)
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                new SyncSetup(Tables));

            var serviceUri = this.Kestrell.Run();

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }


            // ----------------------------------
            // Add rows on server AFTER first sync (so everything should be initialized)
            // ----------------------------------
            var productId = Guid.NewGuid();
            var productName = HelperDatabase.GetRandomName();
            var productNumber = productName.ToUpperInvariant().Substring(0, 10);

            var productCategoryName = HelperDatabase.GetRandomName();
            var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.Add(pc);

                var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber };
                ctx.Add(product);

                await ctx.SaveChangesAsync();
            }

            // ----------------------------------
            // Get changes
            // ----------------------------------
            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);
                var agent = new SyncAgent(client.Provider, webRemoteOrchestrator, options);

                // Ensure scope is created locally
                var clientScope = await agent.LocalOrchestrator.GetClientScopeInfoAsync();

                // get changes from server, without any changes sent from client side
                var changes = await webRemoteOrchestrator.GetEstimatedChangesCountAsync(clientScope);

                Assert.Equal(2, changes.ServerChangesSelected.TotalChangesSelected);

            }
        }

        [Fact]
        public async Task WithBatchingEnabled_WhenSessionIsLostDuringApplyChanges_ChangesAreNotLost()
        {
            // Arrange
            var options = new SyncOptions { BatchSize = 100 };

            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, this.Server.Provider, options);

                var s = await agent.SynchronizeAsync(this.Tables);

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }


            // insert 3000 new products
            var rowsToSend = 3000;
            var productNumber = "12345";

            foreach (var client in Clients)
            {
                var products = Enumerable.Range(1, rowsToSend).Select(i =>
                    new Product { ProductId = Guid.NewGuid(), Name = Guid.NewGuid().ToString("N"), ProductNumber = productNumber + $"_{i}_{client.ProviderType}" });

                using var clientDbCtx = new AdventureWorksContext(client, this.UseFallbackSchema);
                clientDbCtx.Product.AddRange(products);
                await clientDbCtx.SaveChangesAsync();
            }



            // for each client, fake that the sync session is interrupted
            var clientCount = 0;
            foreach (var client in Clients)
            {
                // configure server orchestrator
                this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                    new SyncSetup(Tables));

                int batchIndex = 0;

                // Create server web proxy
                var serverHandler = new RequestDelegate(async context =>
                {
                    var webServerAgent = context.RequestServices.GetService(typeof(WebServerAgent)) as WebServerAgent;

                    webServerAgent.OnHttpSendingResponse(async args =>
                    {
                        // SendChangesInProgress is occuring when server is receiving data from client
                        // We are droping session on the second batch
                        if (args.HttpStep == HttpStep.SendChangesInProgress && batchIndex == 1)
                        {
                            args.HttpContext.Session.Clear();
                            await args.HttpContext.Session.CommitAsync();
                        }
                        if (args.HttpStep == HttpStep.SendChangesInProgress)
                            batchIndex++;

                    });

                    await webServerAgent.HandleRequestAsync(context);
                });


                var serviceUri = this.Kestrell.Run(serverHandler);


                var orch = new WebRemoteOrchestrator(serviceUri);
                var agent = new SyncAgent(client.Provider, orch, options);

                var ex = await Assert.ThrowsAsync<HttpSyncWebException>(() => agent.SynchronizeAsync());

                // Assert
                Assert.NotNull(ex); //"exception required!"
                Assert.Equal("HttpSessionLostException", ex.TypeName);

                await this.Kestrell.StopAsync();

            }

            foreach (var client in Clients)
            {
                // configure server orchestrator
                this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                    new SyncSetup(Tables));

                var serviceUri = this.Kestrell.Run();

                // Act 2: Ensure client can recover
                var agent2 = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);

                var s2 = await agent2.SynchronizeAsync();

                Assert.Equal(rowsToSend, s2.TotalChangesUploaded);
                Assert.Equal(rowsToSend * clientCount, s2.TotalChangesDownloaded);
                Assert.Equal(0, s2.TotalResolvedConflicts);

                clientCount++;

                using var serverDbCtx = new AdventureWorksContext(this.Server);
                var serverCount = serverDbCtx.Product.Count(p => p.ProductNumber.Contains($"{productNumber}_"));
                Assert.Equal(rowsToSend * clientCount, serverCount);

                await this.Kestrell.StopAsync();
            }

        }

        [Fact]
        public async Task WithBatchingEnabled_WhenSessionIsLostDuringGetChanges_ChangesAreNotLost()
        {
            // Arrange
            var options = new SyncOptions { BatchSize = 100 };

            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                new SyncSetup(Tables));

            var serviceUri = this.Kestrell.Run();

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            await this.Kestrell.StopAsync();

            // insert 1000 new products so batching is used
            var rowsToReceive = 1000;
            var productNumber = "12345";

            var products = Enumerable.Range(1, rowsToReceive).Select(i =>
                new Product { ProductId = Guid.NewGuid(), Name = Guid.NewGuid().ToString("N"), ProductNumber = productNumber + $"_{i}" });

            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                serverDbCtx.Product.AddRange(products);
                await serverDbCtx.SaveChangesAsync();
            }

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                new SyncSetup(Tables));

            int batchIndex = 0;

            // Create server web proxy
            var serverHandler = new RequestDelegate(async context =>
            {
                var webServerAgent = context.RequestServices.GetService(typeof(WebServerAgent)) as WebServerAgent;

                // IMPORTANT: Simulate server-side session loss after first batch message is already transmitted
                webServerAgent.OnHttpSendingResponse(async args =>
                {
                    // GetMoreChanges is occuring when server is sending back data to client
                    // We are droping session on the second batch
                    if (args.HttpStep == HttpStep.GetMoreChanges && batchIndex == 1)
                    {
                        args.HttpContext.Session.Clear();
                        await args.HttpContext.Session.CommitAsync();
                    }

                    if (args.HttpStep == HttpStep.GetMoreChanges)
                        batchIndex++;
                });

                await webServerAgent.HandleRequestAsync(context);
            });

            serviceUri = this.Kestrell.Run(serverHandler);

            // for each client, fake that the sync session is interrupted
            foreach (var client in Clients)
            {
                batchIndex = 0;

                // restreint parallelism degrees to be sure the batch index is not downloaded at the end
                // (This will not raise the error if the batchindex 1 is downloaded as the last part)
                var orch = new WebRemoteOrchestrator(serviceUri, maxDownladingDegreeOfParallelism: 1);
                var agent = new SyncAgent(client.Provider, orch, options);


                var ex = await Assert.ThrowsAsync<HttpSyncWebException>(async () =>
                {
                    var r = await agent.SynchronizeAsync();
                });

                // Assert
                Assert.NotNull(ex); //"exception required!"
                Assert.Equal("HttpSessionLostException", ex.TypeName);

                // Act 2: Ensure client can recover
                var agent2 = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);

                var s2 = await agent2.SynchronizeAsync();

                Assert.Equal(0, s2.TotalChangesUploaded);
                Assert.Equal(rowsToReceive, s2.TotalChangesDownloaded);
                Assert.Equal(0, s2.TotalResolvedConflicts);

                using var clientDbCtx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var serverCount = clientDbCtx.Product.Count(p => p.ProductNumber.Contains($"{productNumber}_"));
                Assert.Equal(rowsToReceive, serverCount);
            }

        }

        /// <summary>
        /// Insert one row on server, should be correctly sync on all clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Parallel_Sync_For_TwentyClients(SyncOptions options)
        {
            // create a server database
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            var setup = new SyncSetup(Tables);
            // Provision server, to be sure no clients will try to do something that could break server
            var remoteOrchestrator = new RemoteOrchestrator(this.Server.Provider, options);

            // Ensure schema is ready on server side. Will create everything we need (triggers, tracking, stored proc, scopes)
            var serverScopeInfo = await remoteOrchestrator.GetServerScopeInfoAsync(setup);
            await remoteOrchestrator.ProvisionAsync(serverScopeInfo);

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                new SyncSetup(Tables));

            var serviceUri = this.Kestrell.Run();

            var providers = this.Clients.Select(c => c.ProviderType).Distinct();
            var createdDatabases = new List<(ProviderType ProviderType, string DatabaseName)>();

            var clientProviders = new List<CoreProvider>();
            foreach (var provider in providers)
            {
                for (int i = 0; i < 10; i++)
                {
                    // Create the provider
                    var dbCliName = HelperDatabase.GetRandomName("http_cli_");
                    var localProvider = this.CreateProvider(provider, dbCliName);

                    clientProviders.Add(localProvider);

                    // Create the database
                    await this.CreateDatabaseAsync(provider, dbCliName, true);
                    createdDatabases.Add((provider, dbCliName));
                }
            }

            var allTasks = new List<Task<SyncResult>>();

            // Execute a sync on all clients and add the task to a list of tasks
            foreach (var clientProvider in clientProviders)
            {
                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);
                allTasks.Add(agent.SynchronizeAsync());
            }

            // Await all tasks
            await Task.WhenAll(allTasks);

            foreach (var s in allTasks)
            {
                Assert.Equal(rowsCount, s.Result.TotalChangesDownloaded);
                Assert.Equal(0, s.Result.TotalChangesUploaded);
                Assert.Equal(0, s.Result.TotalResolvedConflicts);
            }


            // Create a new product on server 
            var name = HelperDatabase.GetRandomName();
            var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

            var product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber };

            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                serverDbCtx.Product.Add(product);
                await serverDbCtx.SaveChangesAsync();
            }

            allTasks = new List<Task<SyncResult>>();

            // Execute a sync on all clients to get the new server row
            foreach (var clientProvider in clientProviders)
            {
                var agent = new SyncAgent(clientProvider, new WebRemoteOrchestrator(serviceUri), options);
                allTasks.Add(agent.SynchronizeAsync());
            }

            // Await all tasks
            await Task.WhenAll(allTasks);

            foreach (var s in allTasks)
            {
                Assert.Equal(1, s.Result.TotalChangesDownloaded);
                Assert.Equal(0, s.Result.TotalChangesUploaded);
                Assert.Equal(0, s.Result.TotalResolvedConflicts);
            }

            foreach (var db in createdDatabases)
            {
                try
                {
                    HelperDatabase.DropDatabase(db.ProviderType, db.DatabaseName);
                }
                catch (Exception) { }
            }

        }



        /// <summary>
        /// Insert one row on server, should be correctly sync on all clients
        /// </summary>
        [Fact]
        public async Task Intermitent_Connection_SyncPolicy_RetryOnHttpGettingRequest_ShouldWork()
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                new SyncSetup(Tables));

            var interrupted = new Dictionary<HttpStep, bool>();

            // Create server web proxy
            var serverHandler = new RequestDelegate(async context =>
            {
                var webServerAgent = context.RequestServices.GetService(typeof(WebServerAgent)) as WebServerAgent;

                // When Server Orchestrator send back the response, we will make an interruption
                webServerAgent.OnHttpGettingRequest(args =>
                {
                    if (!interrupted.ContainsKey(args.HttpStep))
                        interrupted.Add(args.HttpStep, false);

                    // interrupt each step to see if it's working
                    if (!interrupted[args.HttpStep])
                    {
                        interrupted[args.HttpStep] = true;
                        throw new TimeoutException($"Timeout exception raised on step {args.HttpStep}");
                    }

                });

                await webServerAgent.HandleRequestAsync(context);
            });

            var serviceUri = this.Kestrell.Run(serverHandler);


            SyncOptions options = new SyncOptions { BatchSize = 10000 };

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);

                var policyRetries = 0;
                webRemoteOrchestrator.OnHttpPolicyRetrying(args =>
                {
                    policyRetries++;
                });

                var agent = new SyncAgent(client.Provider, webRemoteOrchestrator, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
                Assert.InRange(policyRetries, 5, 7);
                interrupted.Clear();
            }

            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                await agent.SynchronizeAsync(Tables);
            }

            var finalRowsCount = this.GetServerDatabaseRowsCount(this.Server);
            foreach (var client in Clients)
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));

        }

        /// <summary>
        /// On Intermittent connection, should work even if server has done its part
        /// </summary>
        [Fact]
        public async Task Intermitent_Connection_SyncPolicy_RetryOnHttpSendingResponse_ShouldWork()
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                new SyncSetup(Tables));

            var interrupted = new Dictionary<HttpStep, bool>();

            // Create server web proxy
            var serverHandler = new RequestDelegate(async context =>
            {
                var webServerAgent = context.RequestServices.GetService(typeof(WebServerAgent)) as WebServerAgent;

                // When Server Orchestrator send back the response, we will make an interruption
                webServerAgent.OnHttpSendingResponse(args =>
                {
                    if (!interrupted.ContainsKey(args.HttpStep))
                        interrupted.Add(args.HttpStep, false);

                    // interrupt each step to see if it's working
                    if (!interrupted[args.HttpStep])
                    {
                        interrupted[args.HttpStep] = true;
                        throw new TimeoutException($"Timeout exception raised on step {args.HttpStep}");
                    }

                });

                await webServerAgent.HandleRequestAsync(context);
            });

            var serviceUri = this.Kestrell.Run(serverHandler);

            SyncOptions options = new SyncOptions { BatchSize = 10000 };

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);

                var policyRetries = 0;
                webRemoteOrchestrator.OnHttpPolicyRetrying(args =>
                {
                    policyRetries++;
                });

                var agent = new SyncAgent(client.Provider, webRemoteOrchestrator, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
                Assert.InRange(policyRetries, 5, 7);
                interrupted.Clear();
            }
        }


        /// <summary>
        /// On Intermittent connection, should work even if server has already applied a batch  and then timeout for some reason 
        /// Client will resend the batch again, but that's ok, since we are merging
        /// </summary>
        [Fact]
        public async Task Intermitent_Connection_SyncPolicy_InsertClientRow_ShouldWork()
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                new SyncSetup(Tables));

            var interruptedBatch = false;

            // Create server web proxy
            var serverHandler = new RequestDelegate(async context =>
            {
                var webServerAgent = context.RequestServices.GetService(typeof(WebServerAgent)) as WebServerAgent;

                // When Server Orchestrator send back the response, we will make an interruption
                webServerAgent.OnHttpSendingResponse(args =>
                {
                    // Throw error when sending changes to server
                    if (args.HttpStep == HttpStep.SendChangesInProgress && !interruptedBatch)
                    {
                        interruptedBatch = true;
                        throw new TimeoutException($"Timeout exception raised on step {args.HttpStep}");
                    }

                });

                await webServerAgent.HandleRequestAsync(context);
            });

            var serviceUri = this.Kestrell.Run(serverHandler);

            SyncOptions options = new SyncOptions { BatchSize = 100 };

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);

                var agent = new SyncAgent(client.Provider, webRemoteOrchestrator, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }


            // Insert one line on each client
            foreach (var client in Clients)
            {
                using var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema);
                for (var i = 0; i < 1000; i++)
                {
                    var name = HelperDatabase.GetRandomName();
                    var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                    var product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber };

                    cliCtx.Product.Add(product);
                }
                await cliCtx.SaveChangesAsync();
            }


            // Sync all clients
            // First client  will upload one line and will download nothing
            // Second client will upload one line and will download one line
            // thrid client  will upload one line and will download two lines
            int download = 0;
            foreach (var client in Clients)
            {
                interruptedBatch = false;

                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);
                var s = await agent.SynchronizeAsync();

                Assert.Equal(download, s.TotalChangesDownloaded);
                Assert.Equal(1000, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

                // We have one batch that has been sent 2 times; it will be merged correctly on server
                Assert.InRange<int>(s.ChangesAppliedOnServer.TotalAppliedChanges, 1000, 2000);
                Assert.Equal(1000, s.ClientChangesSelected.TotalChangesSelected);

                // Get count of rows
                var finalRowsCount = this.GetServerDatabaseRowsCount(this.Server);
                var clientRowsCount = this.GetServerDatabaseRowsCount(client);
                Assert.Equal(finalRowsCount, clientRowsCount);

                download += 1000;

            }
        }


        /// <summary>
        /// Testing if blob are consistent across sync
        /// </summary>
        [Fact]
        public virtual async Task Blob_ShouldBeConsistent_AndSize_ShouldBeMaintained()
        {
            // create a server db and seed it
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                new SyncSetup(Tables));

            var serviceUri = this.Kestrell.Run();

            // Execute a sync on all clients to initialize schemas
            foreach (var client in this.Clients)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);
                var agent = new SyncAgent(client.Provider, webRemoteOrchestrator);
                var s = await agent.SynchronizeAsync();
            }

            // Create a new product on server with a big thumbnail photo
            var name = HelperDatabase.GetRandomName();
            var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

            var product = new Product
            {
                ProductId = Guid.NewGuid(),
                Name = name,
                ProductNumber = productNumber,
                ThumbNailPhoto = new byte[20000]
            };

            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                serverDbCtx.Product.Add(product);
                await serverDbCtx.SaveChangesAsync();
            }

            // Create a new product on client with a big thumbnail photo
            foreach (var client in this.Clients)
            {
                var clientName = HelperDatabase.GetRandomName();
                var clientProductNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var clientProduct = new Product
                {
                    ProductId = Guid.NewGuid(),
                    Name = clientName,
                    ProductNumber = clientProductNumber,
                    ThumbNailPhoto = new byte[20000]
                };

                using (var clientDbCtx = new AdventureWorksContext(client, UseFallbackSchema))
                {
                    clientDbCtx.Product.Add(product);
                    await clientDbCtx.SaveChangesAsync();
                }
            }
            // Two sync to be sure all clients have all rows from all
            foreach (var client in this.Clients)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);
                var agent = new SyncAgent(client.Provider, webRemoteOrchestrator);
                var s = await agent.SynchronizeAsync();
            }
            foreach (var client in this.Clients)
            {
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);
                var agent = new SyncAgent(client.Provider, webRemoteOrchestrator);
                var s = await agent.SynchronizeAsync();
            }


            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var products = await ctx.Product.AsNoTracking().ToListAsync();
                foreach (var p in products)
                {
                    Assert.Equal(20000, p.ThumbNailPhoto.Length);
                }

            }

            foreach (var client in Clients)
            {
                using var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema);

                var products = await cliCtx.Product.AsNoTracking().ToListAsync();
                foreach (var p in products)
                {
                    Assert.Equal(20000, p.ThumbNailPhoto.Length);
                }
            }
        }

    }
}
