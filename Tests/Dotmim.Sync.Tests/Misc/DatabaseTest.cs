using Dotmim.Sync.Tests.Fixtures;
using Microsoft.Data.SqlClient;
#if NET6_0 || NET8_0 
using MySqlConnector;
#elif NETCOREAPP3_1
using MySql.Data.MySqlClient;
#endif
using Npgsql;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;
using System.Threading.Tasks;
using Dotmim.Sync.Tests.Models;
using Dotmim.Sync.Tests.Core;
using System.Data;

namespace Dotmim.Sync.Tests.Misc
{
    public abstract class DatabaseTest : IDisposable, IAsyncLifetime
    {
        private Stopwatch preWorkStopwatch;
        private Stopwatch postWorkStopwatch;

        /// <summary>
        /// Gets the tables used for sync
        /// </summary>
        public virtual string[] GetTables()
        {
            var salesSchema = GetServerProvider().UseFallbackSchema() ? "SalesLT" : null;
            var salesSchemaWithDot = string.IsNullOrEmpty(salesSchema) ? string.Empty : $"{salesSchema}.";

            var tables = new string[]
              {
                $"{salesSchemaWithDot}ProductCategory", $"{salesSchemaWithDot}ProductModel", $"{salesSchemaWithDot}Product",
                "Employee", "Customer", "Address", "CustomerAddress", "EmployeeAddress",
                $"{salesSchemaWithDot}SalesOrderHeader", $"{salesSchemaWithDot}SalesOrderDetail",
                "Posts", "Tags", "PostTag",
                "PricesList", "PricesListCategory", "PricesListDetail", "Log"
              };

            return tables;
        }

        /// <summary>
        /// Gets the setup used for sync without filter
        /// </summary>
        public virtual SyncSetup GetSetup() => new SyncSetup(GetTables());


        /// <summary>
        /// Gets the setup used for sync with filter
        /// </summary>
        public virtual SyncSetup GetFilteredSetup()
        {
            var setup = new SyncSetup(GetTables());

            var salesSchema = GetServerProvider().UseFallbackSchema() ? "SalesLT" : null;
            var salesSchemaWithDot = string.IsNullOrEmpty(salesSchema) ? string.Empty : $"{salesSchema}.";

            // Filter columns
            setup.Tables["Customer"].Columns.AddRange("CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName");
            setup.Tables["Address"].Columns.AddRange("AddressID", "AddressLine1", "City", "PostalCode");

            // Filters clause

            // 1) EASY Way:
            setup.Filters.Add("CustomerAddress", "CustomerID");
            setup.Filters.Add("SalesOrderHeader", "CustomerID", salesSchema);


            // 2) create a custom parameter not based on a column
            var customerFilter = new SetupFilter("Customer");
            customerFilter.AddParameter("custID", DbType.Guid, true);
            customerFilter.AddWhere("CustomerID", "Customer", "custID");
            setup.Filters.Add(customerFilter);

            // 3) Create your own filter

            // Create a filter on table Address
            var addressFilter = new SetupFilter("Address");
            addressFilter.AddParameter("CustomerID", "Customer");
            addressFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "AddressID", "Address", "AddressID");
            addressFilter.AddWhere("CustomerID", "CustomerAddress", "CustomerID");
            setup.Filters.Add(addressFilter);
            // ----------------------------------------------------

            // Create a filter on table SalesLT.SalesOrderDetail
            var salesOrderDetailFilter = new SetupFilter("SalesOrderDetail", salesSchema);
            salesOrderDetailFilter.AddParameter("CustomerID", "Customer");
            salesOrderDetailFilter.AddJoin(Join.Left, $"{salesSchemaWithDot}SalesOrderHeader").On($"{salesSchemaWithDot}SalesOrderHeader", "SalesOrderID", $"{salesSchemaWithDot}SalesOrderDetail", "SalesOrderID");
            salesOrderDetailFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerID", $"{salesSchemaWithDot}SalesOrderHeader", "CustomerID");
            salesOrderDetailFilter.AddWhere("CustomerID", "CustomerAddress", "CustomerID");
            setup.Filters.Add(salesOrderDetailFilter);
            // ----------------------------------------------------

            // 4) Custom Wheres on Product.
            var productFilter = new SetupFilter("Product", salesSchema);

            productFilter.AddCustomWhere("{{{ProductCategoryID}}} IS NOT NULL OR {{{side}}}.{{{sync_row_is_tombstone}}} = 1");
            setup.Filters.Add(productFilter);

            return setup;
        }


        private string sqliteRandomDatabaseName = HelperDatabase.GetRandomName("sqlite_");
        private string sqlServerRandomDatabaseName = HelperDatabase.GetRandomName("server_");
        /// <summary>
        /// Get the server provider
        /// </summary>
        public virtual IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sqlite, sqliteRandomDatabaseName, true);
        }

        /// <summary>
        /// Gets all the server providers
        /// </summary>
        public virtual CoreProvider GetServerProvider()
        {
            return HelperDatabase.GetSyncProvider(ServerProviderType, sqlServerRandomDatabaseName, ServerProviderType == ProviderType.Sql || ServerProviderType == ProviderType.Postgres);
        }

        public abstract ProviderType ServerProviderType { get; }

        /// <summary>
        /// Get filters parameters
        /// </summary>
        public virtual SyncParameters GetFilterParameters() =>
            new SyncParameters(
                 ("CustomerID", AdventureWorksContext.CustomerId1ForFilter),
                 ("custID", AdventureWorksContext.CustomerId1ForFilter)
            );

        public virtual DatabaseServerFixture Fixture { get; }
        public virtual ITestOutputHelper Output { get; }
        public virtual XunitTest Test { get; }
        public virtual Stopwatch Stopwatch { get; private set; }

        /// <summary>
        /// Gets or Sets the Kestrel server used to server http queries
        /// </summary>
        public KestrelTestServer Kestrel { get; set; }

        /// <summary>
        /// Gets if fiddler is in use
        /// </summary>
        public bool UseFiddler { get; set; }

        public DatabaseTest(ITestOutputHelper output, DatabaseServerFixture fixture)
        {
            this.Fixture = fixture;
            // Getting the test running
            this.Output = output;
            var type = output.GetType();
            var testMember = type.GetField("test", BindingFlags.Instance | BindingFlags.NonPublic);
            this.Test = (XunitTest)testMember.GetValue(output);

            // Create a kestrel server
            this.Kestrel = new KestrelTestServer(this.UseFiddler);
        }

        public async Task InitializeAsync()
        {
            preWorkStopwatch = Stopwatch.StartNew();

            SqlConnection.ClearAllPools();
            MySqlConnection.ClearAllPools();
            NpgsqlConnection.ClearAllPools();

            await CreateDatabasesAsync();

            preWorkStopwatch.Stop();

            this.Stopwatch = Stopwatch.StartNew();

        }
        public Task DisposeAsync() => Task.CompletedTask;


        //private void ResetClientsTables()
        //{
        //    // Drop DMS metadatas and truncate clients tables
        //    foreach (var clientProvider in GetClientProviders())
        //    {
        //        // drop all DMS tracking tables & metadatas
        //        clientProvider.DropAllTablesAsync(false).GetAwaiter().GetResult();
        //        // truncate all tables
        //        clientProvider.EmptyAllTablesAsync().GetAwaiter().GetResult();
        //    }
        //}

        private async Task CreateDatabasesAsync()
        {
            var (serverProviderType, serverDatabaseName) = HelperDatabase.GetDatabaseType(GetServerProvider());
            var serverProvider = GetServerProvider();
            using (var ctx = new AdventureWorksContext(serverProvider, true))
            {
                await ctx.Database.EnsureCreatedAsync();

                if (serverProviderType == ProviderType.Sql)
                    await HelperDatabase.ActivateChangeTracking(serverDatabaseName);
            }

            foreach (var clientProvider in GetClientProviders())
            {
                var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);
                using var cliCtx = new AdventureWorksContext(clientProvider);
                await cliCtx.Database.EnsureCreatedAsync();

                if (clientProviderType == ProviderType.Sql)
                    await HelperDatabase.ActivateChangeTracking(clientDatabaseName);
            }
        }

        public void OutputCurrentState(string subCategory = null)
        {
            var t = string.IsNullOrEmpty(subCategory) ? "" : $" - {subCategory}";

            //var methodParameters = this.Test.TestCase.Method.GetParameters().ToList();
            //var parameters = new StringBuilder();
            //if (methodParameters != null && methodParameters.Count > 0)
            //    foreach (ReflectionParameterInfo methodParameter in methodParameters)
            //        parameters.Append($"{methodParameter.Name}:{methodParameter.ParameterInfo.DefaultValue}. ");

            //var overallTime = $"[Overall :{Fixture.OverallStopwatch.Elapsed.Minutes}:{Fixture.OverallStopwatch.Elapsed.Seconds}.{Fixture.OverallStopwatch.Elapsed.Milliseconds}]";
            var preWorkEllapsedTime = $"[Pre :{this.preWorkStopwatch.Elapsed.Minutes}:{this.preWorkStopwatch.Elapsed.Seconds}.{this.preWorkStopwatch.Elapsed.Milliseconds}]";
            var postWorkEllapsedTime = $"[Post :{this.postWorkStopwatch.Elapsed.Minutes}:{this.postWorkStopwatch.Elapsed.Seconds}.{this.postWorkStopwatch.Elapsed.Milliseconds}]";
            var workEllapsedTime = $"[Test: {this.Stopwatch.Elapsed.Minutes}:{this.Stopwatch.Elapsed.Seconds}.{this.Stopwatch.Elapsed.Milliseconds}]";
            var testClass = this.Test.TestCase.TestMethod.TestClass.Class as ReflectionTypeInfo;

            string clientsDbName = "";
            string comma = "";
            foreach (var cliProvider in GetClientProviders())
            {
                clientsDbName += $"{comma}{cliProvider.GetDatabaseName()}";
                comma = "-";
            }

            var serverDbName = $"[Server {GetServerProvider().GetDatabaseName()}]";
            clientsDbName = $"[Clients {clientsDbName}]";

            t = $"{testClass.Type.Name}.{this.Test.TestCase.Method.Name}{t}: {serverDbName}-{clientsDbName} - {preWorkEllapsedTime}-{postWorkEllapsedTime} - {workEllapsedTime}.";
            Console.WriteLine(t);
            Debug.WriteLine(t);
            this.Output.WriteLine(t);
        }

        public void Dispose()
        {
            this.Stopwatch.Stop();

            this.postWorkStopwatch = Stopwatch.StartNew();

            var serverProvider = GetServerProvider();
            if (serverProvider.UseShouldDropDatabase())
            {
                var (serverProviderType, serverDatabaseName) = HelperDatabase.GetDatabaseType(serverProvider);
                HelperDatabase.DropDatabase(serverProviderType, serverDatabaseName);
            }

            foreach (var clientProvider in GetClientProviders())
            {
                if (clientProvider.UseShouldDropDatabase())
                {
                    // HelperDatabase.GetDatabaseType(clientProvider);
                    var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);

                    HelperDatabase.DropDatabase(clientProviderType, clientDatabaseName);
                }
            }

            this.postWorkStopwatch.Stop();

            OutputCurrentState();
        }

    }
}
