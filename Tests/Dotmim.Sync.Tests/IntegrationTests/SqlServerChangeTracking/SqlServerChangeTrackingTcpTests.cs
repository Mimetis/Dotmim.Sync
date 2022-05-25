
using Dotmim.Sync.MariaDB;
using Dotmim.Sync.MySql;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests.IntegrationTests
{
    public class SqlServerChangeTrackingTcpTests : TcpTests
    {
        public override List<ProviderType> ClientsType => new List<ProviderType>
            {  ProviderType.Sql};

        public SqlServerChangeTrackingTcpTests(HelperProvider fixture, ITestOutputHelper output) : base(fixture, output)
        {
        }

        public override string[] Tables => new string[]
        {
            "SalesLT.ProductCategory", "SalesLT.ProductModel", "SalesLT.Product", "Employee", "Customer", "Address", "CustomerAddress", "EmployeeAddress",
            "SalesLT.SalesOrderHeader", "SalesLT.SalesOrderDetail", "Posts", "Tags", "PostTag",
            "PricesList", "PricesListCategory", "PricesListDetail", "Log"
        };


        public override ProviderType ServerType =>
            ProviderType.Sql;

        public override CoreProvider CreateProvider(ProviderType providerType, string dbName)
        {
            var cs = HelperDatabase.GetConnectionString(providerType, dbName);
            switch (providerType)
            {
                case ProviderType.MySql:
                    return new MySqlSyncProvider(cs);
                case ProviderType.MariaDB:
                    return new MariaDBSyncProvider(cs);
                case ProviderType.Sqlite:
                    return new SqliteSyncProvider(cs);
                case ProviderType.Sql:
                default:
                    return new SqlSyncChangeTrackingProvider(cs);
            }
        }

  
        public override async Task EnsureDatabaseSchemaAndSeedAsync((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) t
            , bool useSeeding = false, bool useFallbackSchema = false)
        {
            AdventureWorksContext ctx = null;
            try
            {
                ctx = new AdventureWorksContext(t, useFallbackSchema, useSeeding);
                await ctx.Database.EnsureCreatedAsync();

                if (t.ProviderType == ProviderType.Sql)
                    await this.ActivateChangeTracking(t.DatabaseName);
            }
            catch (Exception)
            {
            }
            finally
            {
                if (ctx != null)
                    ctx.Dispose();
            }
        }

        public override async Task CreateDatabaseAsync(ProviderType providerType, string dbName, bool recreateDb = true)
        {
            await HelperDatabase.CreateDatabaseAsync(providerType, dbName, recreateDb);

            if (providerType == ProviderType.Sql)
                await this.ActivateChangeTracking(dbName);
        }


        private async Task ActivateChangeTracking(string dbName)
        {

            var c = new SqlConnection(Setup.GetSqlDatabaseConnectionString(dbName));

            // Check if we are using change tracking and it's enabled on the source
            var isChangeTrackingEnabled = await SqlManagementUtils.IsChangeTrackingEnabledAsync(c, null).ConfigureAwait(false);

            if (isChangeTrackingEnabled)
                return;

            using var masterConnection = new SqlConnection(Setup.GetSqlDatabaseConnectionString("master"));

            var script = $"ALTER DATABASE {dbName} SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)";


            masterConnection.Open();

            using (var cmdCT = new SqlCommand(script, masterConnection))
                await cmdCT.ExecuteNonQueryAsync();

            masterConnection.Close();
        }

        /// <summary>
        /// Get the server database rows count
        /// </summary>
        /// <returns></returns>
        public override int GetServerDatabaseRowsCount((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) t)
        {
            int totalCountRows = 0;

            using (var serverDbCtx = new AdventureWorksContext(t))
            {
                totalCountRows += serverDbCtx.Address.Count();
                totalCountRows += serverDbCtx.Customer.Count();
                totalCountRows += serverDbCtx.CustomerAddress.Count();
                totalCountRows += serverDbCtx.Employee.Count();
                totalCountRows += serverDbCtx.EmployeeAddress.Count();
                totalCountRows += serverDbCtx.Log.Count();
                totalCountRows += serverDbCtx.Posts.Count();
                totalCountRows += serverDbCtx.PostTag.Count();
                totalCountRows += serverDbCtx.PricesList.Count();
                totalCountRows += serverDbCtx.PricesListCategory.Count();
                totalCountRows += serverDbCtx.PricesListDetail.Count();
                totalCountRows += serverDbCtx.Product.Count();
                totalCountRows += serverDbCtx.ProductCategory.Count();
                totalCountRows += serverDbCtx.ProductModel.Count();
                totalCountRows += serverDbCtx.SalesOrderDetail.Count();
                totalCountRows += serverDbCtx.SalesOrderHeader.Count();
                //totalCountRows += serverDbCtx.Sql.Count();
                totalCountRows += serverDbCtx.Tags.Count();
            }

            return totalCountRows;
        }


        /// <summary>
        /// Testing an insert / update on a table where a column is not part of the sync setup, and should stay alive after a sync
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
#pragma warning disable xUnit1026 // Theory methods should use all of their parameters
        public override Task OneColumn_NotInSetup_AfterCleanMetadata_IsTracked_ButNotUpdated(SyncOptions options)
#pragma warning restore xUnit1026 // Theory methods should use all of their parameters
            => Task.CompletedTask;

        ///// <summary>
        ///// Since we do not have control on the change tracking mechanism, any row updated will be marked as updated
        ///// Even if the value is the same or if the column is not part of sync setup
        ///// </summary>
        //public override Task OneColumn_NotInSetup_AfterCleanMetadata_IsTracked_ButNotUpdated(SyncOptions options)
        //{
        //    // TODO : Maybe this method now works also for change tracking
        //    // Since all triggers are now agnostic to columns
        //    return Task.CompletedTask;
        //}

        ///// <summary>
        ///// Testing that an upate from the server does not replace, but just update the local row, so that columns that are not included in the sync are not owervritten/cleared
        ///// NOTE: This test is slightly different than the base as CT marks all columns as changed after an insert,
        ///// so this only tests updates after the insert is synchronized.
        ///// </summary>
        //[Theory]
        //[ClassData(typeof(SyncOptionsData))]
        //public async override Task OneColumn_NotInSetup_IfServerSendsChanges_UpdatesLocalRow_AndDoesNotClear_OneColumn(SyncOptions options)
        //{
        //    // create a server schema with seeding
        //    await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

        //    // create empty client databases with schema
        //    foreach (var client in this.Clients)
        //        await this.EnsureDatabaseSchemaAndSeedAsync(client, false, UseFallbackSchema);


        //    // this Guid will be updated on the client
        //    var clientGuid = Guid.NewGuid();

        //    // Get server Guid value, that should not change
        //    Guid? serverGuid;
        //    using (var serverDbCtx = new AdventureWorksContext(this.Server))
        //    {
        //        var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == 1);
        //        serverGuid = address.Rowguid;
        //    }


        //    // Execute a sync on all clients to initialize client and server schema 
        //    foreach (var client in Clients)
        //    {
        //        var setup = new SyncSetup(new string[] { "Address" });

        //        // Add all columns to address except Rowguid and ModifiedDate
        //        setup.Tables["Address"].Columns.AddRange(new string[] { "AddressId", "AddressLine1", "AddressLine2", "City", "StateProvince", "CountryRegion", "PostalCode" });

        //        var agent = new SyncAgent(client.Provider, Server.Provider, options);

        //        var s = await agent.SynchronizeAsync(setup);

        //        // The first sync has inserted records,
        //        // so now we have to synchronize again so that the next sync will have the latest change version.
        //        // This does mean that when the sync inserts records and then before the next sync a column is updated that is not part
        //        // of the sync process, it will still be synchronized.
        //        var s2 = await agent.SynchronizeAsync(setup);

        //        // Editing Rowguid on client. This column is not part of the setup
        //        // So far, it should not be uploaded to server
        //        using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);

        //        var cliAddress = await ctx.Address.SingleAsync(a => a.AddressId == 1);

        //        // Now Update on client this address with a rowGuid
        //        cliAddress.Rowguid = clientGuid;

        //        await ctx.SaveChangesAsync();
        //    }

        //    // Act
        //    // Change row on server and make sure that client rows are just UPDATED and not REPLACED
        //    using (var serverDbCtx = new AdventureWorksContext(this.Server))
        //    {
        //        var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == 1);
        //        address.City = "Mimecity";
        //        await serverDbCtx.SaveChangesAsync();
        //    }

        //    foreach (var client in Clients)
        //    {
        //        var setup = new SyncSetup(new string[] { "Address" });

        //        // Add all columns to address except Rowguid and ModifiedDate
        //        setup.Tables["Address"].Columns.AddRange(new string[] { "AddressId", "AddressLine1", "AddressLine2", "City", "StateProvince", "CountryRegion", "PostalCode" });

        //        var agent = new SyncAgent(client.Provider, Server.Provider, options);
        //        var s = await agent.SynchronizeAsync(setup);

        //        // "Mimecity" change should be received from server
        //        Assert.Equal(1, s.TotalChangesDownloaded);

        //        // No upload since Rowguid is not part of SyncSetup (and trigger shoul not add a line)
        //        Assert.Equal(0, s.TotalChangesUploaded);
        //        Assert.Equal(0, s.TotalResolvedConflicts);

        //        // check row on client should not have been updated 
        //        using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
        //        var cliAddress = await ctx.Address.AsNoTracking().SingleAsync(a => a.AddressId == 1);

        //        Assert.Equal(clientGuid, cliAddress.Rowguid);
        //        Assert.Equal("Mimecity", cliAddress.City);
        //    }


        //    // Check on server guid has not been uploaded
        //    using (var serverDbCtx = new AdventureWorksContext(this.Server))
        //    {
        //        var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == 1);
        //        Assert.Equal(serverGuid, address.Rowguid);
        //    }
        //}
    }
}
