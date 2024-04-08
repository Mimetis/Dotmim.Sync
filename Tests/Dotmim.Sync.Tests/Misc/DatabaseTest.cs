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
using System.Text;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;
using System.Linq;
using System.Threading.Tasks;
using Dotmim.Sync.Tests.Models;
using System.Xml.Linq;
using Dotmim.Sync.Tests.Core;

namespace Dotmim.Sync.Tests.Misc
{
    public abstract class DatabaseTest : IDisposable
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
            setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName" });
            setup.Tables["Address"].Columns.AddRange(new string[] { "AddressID", "AddressLine1", "City", "PostalCode" });

            // Filters clause

            // 1) EASY Way:
            setup.Filters.Add("CustomerAddress", "CustomerID");
            setup.Filters.Add("SalesOrderHeader", "CustomerID", salesSchema);


            // 2) Same, but decomposed in 3 Steps
            var customerFilter = new SetupFilter("Customer");
            customerFilter.AddParameter("CustomerID", "Customer", true);
            customerFilter.AddWhere("CustomerID", "Customer", "CustomerID");
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
        /// Gets all the client providers. By default, SQLIte is always required
        /// </summary>
        public virtual CoreProvider GetServerProvider()
        {
            return HelperDatabase.GetSyncProvider(ServerProviderType, sqlServerRandomDatabaseName, ServerProviderType == ProviderType.Sql || ServerProviderType == ProviderType.Postgres);
        }

        public abstract ProviderType ServerProviderType { get; }

        /// <summary>
        /// Get filters parameters
        /// </summary>
        public virtual SyncParameters GetFilterParameters() => new SyncParameters(("CustomerID", AdventureWorksContext.CustomerId1ForFilter));

        public virtual DatabaseServerFixture Fixture { get; }
        public virtual ITestOutputHelper Output { get; }
        public virtual XunitTest Test { get; }
        public virtual Stopwatch Stopwatch { get; }

        /// <summary>
        /// Gets or Sets the Kestrell server used to server http queries
        /// </summary>
        public KestrellTestServer Kestrell { get; set; }

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

            // Create a kestrell server
            this.Kestrell = new KestrellTestServer(this.UseFiddler);

            preWorkStopwatch = Stopwatch.StartNew();

            SqlConnection.ClearAllPools();
            MySqlConnection.ClearAllPools();
            NpgsqlConnection.ClearAllPools();

            CreateDatabases();

            preWorkStopwatch.Stop();

            this.Stopwatch = Stopwatch.StartNew();
        }

        private void ResetClientsTables()
        {
            // Drop DMS metadatas and truncate clients tables
            foreach (var clientProvider in GetClientProviders())
            {
                // drop all DMS tracking tables & metadatas
                clientProvider.DropAllTablesAsync(false).GetAwaiter().GetResult();
                // truncate all tables
                clientProvider.EmptyAllTablesAsync().GetAwaiter().GetResult();
            }
        }

        private void CreateDatabases()
        {
            var (serverProviderType, serverDatabaseName) = HelperDatabase.GetDatabaseType(GetServerProvider());

            new AdventureWorksContext(GetServerProvider(), true).Database.EnsureCreated();

            if (serverProviderType == ProviderType.Sql)
                HelperDatabase.ActivateChangeTracking(serverDatabaseName).GetAwaiter().GetResult();

            foreach (var clientProvider in GetClientProviders())
            {
                var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);
                new AdventureWorksContext(clientProvider).Database.EnsureCreated();
                if (clientProviderType == ProviderType.Sql)
                    HelperDatabase.ActivateChangeTracking(clientDatabaseName).GetAwaiter().GetResult();
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
