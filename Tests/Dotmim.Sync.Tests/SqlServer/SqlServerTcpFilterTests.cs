
using Dotmim.Sync.MySql;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests
{
    public class SqlServerTcpFilterTests : TcpFilterTests
    {
        public SqlServerTcpFilterTests(HelperProvider fixture, ITestOutputHelper output) : base(fixture, output)
        {
        }

        public override SyncSetup FilterSetup
        {
            get
            {
                var setup = new SyncSetup(new string[] { "Customer", "Address", "CustomerAddress", "SalesLT.SalesOrderHeader", "SalesLT.SalesOrderDetail" });

                // Filter columns
                setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName" });
                setup.Tables["Address"].Columns.AddRange(new string[] { "AddressID", "AddressLine1", "City", "PostalCode" });


                // Filters clause

                // 1) EASY Way:
                //setup.Filters.Add("CustomerAddress", "CustomerID");
                //setup.Filters.Add("SalesOrderHeader", "CustomerID", "SalesLT");


                // 2) Same, but decomposed in 3 Steps

                // Create a filter on table Customer
                // Shortcut to the next 3 commands : setup.Filters.Add("Customer", "CustomerID");
                var customerFilter = new SetupFilter("Customer");
                // Create a parameter based on column Customer Id in table Customer
                customerFilter.AddParameter("CustomerId", "Customer", true);
                // add the side where expression, allowing to be null
                customerFilter.AddWhere("CustomerId", "Customer", "CustomerId");

                setup.Filters.Add(customerFilter);
                // 3) Create your own filter

                // Create a filter on table Address
                var addressFilter = new SetupFilter("Address");

                // Creating parameters
                // -------------------------------------------------
                // The 2 next parameters will generate something like :
                //
                // ALTER PROCEDURE [dbo].[Address_changes]
                //   @sync_min_timestamp bigint,
                //   @sync_scope_id uniqueidentifier,
                //   @CustomerID uniqueidentier,
                //   @Gender varchar(2) = 'M'


                // Adding a parameter based on the [Customer].[CustomerID] column
                // Using a matching on a column allows us on not defining any column type, size and so on...
                addressFilter.AddParameter("CustomerID", "Customer");

                // Adding a customer parameter, where we specify everything
                // this parameter will be add as :
                // For SQL : @Gender varchar(2) = 'M'
                // For MySql : in_Gender varchar(2) = 'M'
                addressFilter.AddParameter("Gender", DbType.AnsiStringFixedLength, false, "M", 2);
                // -------------------------------------------------


                // Creating joins
                // ----------------------------------------------------
                // The 2 next joins will generate
                // LEFT JOIN [CustomerAddress] on [CustomerAddress].[AddressID] = [side].[AddressID]
                // LEFT JOIN [Customer] on [Customer].[AddressID] = [side].[AddressID]

                addressFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "AddressId", "Address", "AddressId");

                // Will generate

                // Will generate
                // LEFT JOIN [Customer] on [Customer].[CustomerID] = [CustomerAddress].[CustomerID]
                addressFilter.AddJoin(Join.Left, "Customer").On("Customer", "CustomerID", "CustomerAddress", "CustomerID");
                // ----------------------------------------------------




                return setup;
            }
        }

        public override List<SyncParameter> FilterParameters => new List<SyncParameter>
        {
                new SyncParameter("CustomerID", AdventureWorksContext.CustomerIdForFilter),
        };


        public override List<ProviderType> ClientsType => new List<ProviderType>
            { ProviderType.Sql};

        public override ProviderType ServerType =>
            ProviderType.Sql;

        public override async Task EnsureDatabaseSchemaAndSeedAsync((string DatabaseName, ProviderType ProviderType, IOrchestrator Orchestrator) t, bool useSeeding = false, bool useFallbackSchema = false)
        {
            AdventureWorksContext ctx = null;
            try
            {
                ctx = new AdventureWorksContext(t, useFallbackSchema, useSeeding);
                await ctx.Database.EnsureCreatedAsync();

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

        public override Task CreateDatabaseAsync(ProviderType providerType, string dbName, bool recreateDb = true)
        {
            return HelperDatabase.CreateDatabaseAsync(providerType, dbName, recreateDb);
        }

        /// <summary>
        /// Get the server database rows count
        /// </summary>
        /// <returns></returns>
        public override int GetServerDatabaseRowsCount((string DatabaseName, ProviderType ProviderType, IOrchestrator Orchestrator) t)
        {
            int totalCountRows = 0;

            using (var serverDbCtx = new AdventureWorksContext(t))
            {
                totalCountRows += serverDbCtx.Address.Count();
                totalCountRows += serverDbCtx.Customer.Where(c => c.CustomerId == AdventureWorksContext.CustomerIdForFilter).Count();
                totalCountRows += serverDbCtx.CustomerAddress.Where(c => c.CustomerId == AdventureWorksContext.CustomerIdForFilter).Count();
                totalCountRows += serverDbCtx.SalesOrderDetail.Count();
                totalCountRows += serverDbCtx.SalesOrderHeader.Where(c => c.CustomerId == AdventureWorksContext.CustomerIdForFilter).Count();
            }

            return totalCountRows;
        }


    }
}
