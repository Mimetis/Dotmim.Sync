using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;

using Dotmim.Sync.SqlServer;
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

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests
{
    //[TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    public abstract partial class TcpFilterTests : IClassFixture<HelperProvider>, IDisposable
    {
        /// <summary>
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Scenario_Using_ExistingClientDatabase_ProvisionDeprovision_WithoutAccessToServerSide(SyncOptions options)
        {
            // This test works only if we have the same exact provider on both sides

            // create client orchestrator that is the same as server
            var clientDatabaseName = HelperDatabase.GetRandomName("tcpfilt_cli_");
            var clientProvider = this.CreateProvider(this.ServerType, clientDatabaseName);

            var client = (clientDatabaseName, Server.ProviderType, Provider: clientProvider);

            // create a client schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(client, false, UseFallbackSchema);

            // Since we don't have access to remote orchestrator, 
            // we can simulate a server scope
            var localOrchestrator = new LocalOrchestrator(client.Provider, options);

            // Get the local scope
            var localScopeInfo = await localOrchestrator.GetScopeInfoAsync();

            // getting local scope did not get the schema
            var schema = await localOrchestrator.GetSchemaAsync(this.FilterSetup);

            // getting local schema from these provider will not fill the schema name for each table
            // and we need the exact same name even if it's not used on client
            if (client.ProviderType == ProviderType.MySql || client.ProviderType == ProviderType.MariaDB || client.ProviderType == ProviderType.Sqlite)
            {
                foreach (var table in schema.Tables)
                {
                    var setupTable = this.FilterSetup.Tables.First(t => t.TableName == table.TableName);
                    table.SchemaName = setupTable.SchemaName;
                }
            }

            // Simulate a server scope
            var serverScope = new ScopeInfo
            {
                Name = localScopeInfo.Name,
                Schema = schema,
                Setup = this.FilterSetup,
                Version = localScopeInfo.Version
            };

            // just check interceptor
            var onTableCreatedCount = 0;
            localOrchestrator.OnTableCreated(args => onTableCreatedCount++);

            // Provision the database with all tracking tables, stored procedures, triggers and scope
            var clientScope = await localOrchestrator.ProvisionAsync(serverScope);

            //--------------------------
            // ASSERTION
            //--------------------------

            // check if scope table is correctly created
            var scopeInfoTableExists = await localOrchestrator.ExistScopeInfoTableAsync();
            Assert.True(scopeInfoTableExists);

            // get the db manager
            foreach (var setupTable in this.FilterSetup.Tables)
            {
                Assert.True(await localOrchestrator.ExistTrackingTableAsync(clientScope, setupTable.TableName, setupTable.SchemaName));

                Assert.True(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Delete));
                Assert.True(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Insert));
                Assert.True(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Update));

                if (client.ProviderType == ProviderType.Sql)
                {
                    Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkDeleteRows));
                    Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkTableType));
                    Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkUpdateRows));
                }

                Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.DeleteMetadata));
                Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.DeleteRow));
                Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.Reset));
                Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChanges));
                Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChanges));
                Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectRow));
                Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.UpdateRow));

                // Filters here
                Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChangesWithFilters));
                Assert.True(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChangesWithFilters));

            }

            //localOrchestrator.OnTableProvisioned(null);

            //// Deprovision the database with all tracking tables, stored procedures, triggers and scope
            await localOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.ScopeInfo | SyncProvision.TrackingTable);

            // check if scope table is correctly created
            scopeInfoTableExists = await localOrchestrator.ExistScopeInfoTableAsync();
            Assert.False(scopeInfoTableExists);

            // get the db manager
            foreach (var setupTable in this.FilterSetup.Tables)
            {
                Assert.False(await localOrchestrator.ExistTrackingTableAsync(clientScope, setupTable.TableName, setupTable.SchemaName));

                Assert.False(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Delete));
                Assert.False(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Insert));
                Assert.False(await localOrchestrator.ExistTriggerAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Update));

                if (client.ProviderType == ProviderType.Sql)
                {
                    Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkDeleteRows));
                    Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkTableType));
                    Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkUpdateRows));
                }

                Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.DeleteMetadata));
                Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.DeleteRow));
                Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.Reset));
                Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChanges));
                Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChanges));
                Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectRow));
                Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.UpdateRow));

                // check filters are deleted
                Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChangesWithFilters));
                Assert.False(await localOrchestrator.ExistStoredProcedureAsync(clientScope, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChangesWithFilters));

            }
        }


        /// <summary>
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Scenario_MultiScopes_SameTables_DifferentFilters(SyncOptions options)
        {
            // This test works only if we have the same exact provider on both sides

            // create client orchestrator that is the same as server
            var clientDatabaseName = HelperDatabase.GetRandomName("tcpfilt_cli_");
            var clientProvider = this.CreateProvider(this.ServerType, clientDatabaseName);

            // create a client database
            await this.CreateDatabaseAsync(Server.ProviderType, clientDatabaseName, true);

            // Get the correct names for ProductCategory and Product
            var productCategoryTableName = this.Server.ProviderType == ProviderType.Sql ? "SalesLT.ProductCategory" : "ProductCategory";
            var productTableName = this.Server.ProviderType == ProviderType.Sql ? "SalesLT.Product" : "Product";

            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // Step 1: Create a default scope and Sync clients
            // Note we are not including the [Attribute With Space] column
            var setup = new SyncSetup(productCategoryTableName, productTableName);

            setup.Tables[productCategoryTableName].Columns.AddRange(
                new string[] { "ProductCategoryId", "Name", "rowguid", "ModifiedDate" });

            var schemaName = this.Server.ProviderType == ProviderType.Sql ? "SalesLT" : null;

            // Add filters
            var productFilter = new SetupFilter("Product", schemaName);
            productFilter.AddParameter("ProductCategoryID", "Product", schemaName);
            productFilter.AddWhere("ProductCategoryID", "Product", "ProductCategoryID", schemaName);

            var productCategoryFilter = new SetupFilter("ProductCategory", schemaName);
            productCategoryFilter.AddParameter("ProductCategoryID", "ProductCategory", schemaName);
            productCategoryFilter.AddWhere("ProductCategoryID", "ProductCategory", "ProductCategoryID", schemaName);

            setup.Filters.Add(productCategoryFilter);
            setup.Filters.Add(productFilter);

            // ------------------------------------------------
            var paramMountb = new SyncParameters(("ProductCategoryID", "MOUNTB"));
            var paramRoadfr = new SyncParameters(("ProductCategoryID", "ROADFR"));

            // create agent with filtered tables and parameter
            var agent = new SyncAgent(clientProvider, Server.Provider, options);
            
            var rTourb = await agent.SynchronizeAsync("Mountb", setup, paramMountb);
            var rRoadfr = await agent.SynchronizeAsync("Roadfr", setup, paramRoadfr);

            Assert.Equal(8, rTourb.TotalChangesDownloaded);
            Assert.Equal(8, rTourb.TotalChangesApplied);
            Assert.Equal(3, rRoadfr.TotalChangesDownloaded);
            Assert.Equal(3, rRoadfr.TotalChangesApplied);
        }

    }
}
