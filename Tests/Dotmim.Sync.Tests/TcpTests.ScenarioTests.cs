using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.SqlServer.Manager;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Tests.Models;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
#if NET5_0 || NET6_0 || NETCOREAPP3_1
using MySqlConnector;
#elif NETCOREAPP2_1
using MySql.Data.MySqlClient;
#endif
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests
{
    //[TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    public abstract partial class TcpTests : IClassFixture<HelperProvider>, IDisposable
    {

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public virtual async Task Scenario_MigrationAddingColumnsAndTableAsync(SyncOptions options)
        {

            // create a server db and seed it
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            var customerRowsCount = 0;
            // Get count of customer's rows
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
                customerRowsCount += serverDbCtx.Customer.Count();

            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                // --------------------------
                // Step 1: Create a default scope and Sync clients
                var setup = new SyncSetup(new string[] { "Customer" });
                setup.Tables["Customer"].Columns.AddRange(
                    new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName" });

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(customerRowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);

                // --------------------------
                // Step 2 : Add a new scope to server with this new columns
                //          Creating a new scope called "V1" on server
                var setupV1 = new SyncSetup(new string[] { "Customer" });

                setupV1.Tables["Customer"].Columns.AddRange(
                    new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName",
                    "ModifiedDate"});

                // Provision this new scope
                var serverOrchestrator = new RemoteOrchestrator(Server.Provider, options);
                await serverOrchestrator.ProvisionAsync("v1", setupV1);

                // add column to client
                await AddColumnsToCustomerAsync(client.Provider);

                // Provision the "v1" scope on the client with the new setup
                // 
                await agent.LocalOrchestrator.ProvisionAsync("v1", setupV1);

                // make a reinit sync

                var resultV1 = await agent.SynchronizeAsync("v1", SyncType.Reinitialize);

                Assert.Equal(customerRowsCount, resultV1.TotalChangesDownloaded);
                Assert.Equal(0, resultV1.TotalChangesUploaded);

            }


        }

        private static async Task AddColumnsToCustomerAsync(CoreProvider provider)
        {
            var commandText = @"ALTER TABLE Customer ADD ModifiedDate datetime NULL";

            var connection = provider.CreateConnection();

            connection.Open();

            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;

            await command.ExecuteNonQueryAsync();

            connection.Close();
        }


    }
}
