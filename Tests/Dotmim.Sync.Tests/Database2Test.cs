using Dotmim.Sync.Tests.Fixtures;
using Microsoft.Data.SqlClient;
using MySqlConnector;
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

namespace Dotmim.Sync.Tests
{
    public abstract class Database2Test : IDisposable
    {
        private Stopwatch initializeStopwatch;

        /// <summary>
        /// Gets the tables used for sync
        /// </summary>
        public virtual string[] GetTables()
        {
            string salesSchema = GetServerProvider().UseFallbackSchema() ? "SalesLT." : "";

            var tables = new string[]
              {
                $"{salesSchema}ProductCategory", $"{salesSchema}ProductModel", $"{salesSchema}Product",
                "Employee", "Customer", "Address", "CustomerAddress", "EmployeeAddress",
                $"{salesSchema}SalesOrderHeader", $"{salesSchema}SalesOrderDetail",
                "Posts", "Tags", "PostTag",
                "PricesList", "PricesListCategory", "PricesListDetail", "Log"
              };

            return tables;
        }

        /// <summary>
        /// Gets the setup used for sync
        /// </summary>
        public virtual SyncSetup GetSetup() => new SyncSetup(GetTables());

        /// <summary>
        /// Get the server provider
        /// </summary>
        public abstract CoreProvider GetServerProvider();

        /// <summary>
        /// Gets all the client providers
        /// </summary>
        public abstract IEnumerable<CoreProvider> GetClientProviders();

        /// <summary>
        /// Get filters parameters
        /// </summary>
        public virtual SyncParameters GetFilterParameters() => null;

        public virtual DatabaseServerFixture2 Fixture { get; }
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

        public Database2Test(ITestOutputHelper output, DatabaseServerFixture2 fixture)
        {
            this.Fixture = fixture;
            // Getting the test running
            this.Output = output;
            var type = output.GetType();
            var testMember = type.GetField("test", BindingFlags.Instance | BindingFlags.NonPublic);
            this.Test = (XunitTest)testMember.GetValue(output);

            // Create a kestrell server
            this.Kestrell = new KestrellTestServer(this.UseFiddler);

            initializeStopwatch = Stopwatch.StartNew();

            SqlConnection.ClearAllPools();
            MySqlConnection.ClearAllPools();
            NpgsqlConnection.ClearAllPools();

            CreateDatabases();

            initializeStopwatch.Stop();

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

            HelperDatabase.DropDatabase(serverProviderType, serverDatabaseName);

            foreach (var clientProvider in GetClientProviders())
            {
                var (clientProviderType, clientDatabaseName) = HelperDatabase.GetDatabaseType(clientProvider);
                HelperDatabase.DropDatabase(clientProviderType, clientDatabaseName);
            }


            new AdventureWorksContext(GetServerProvider(), true).Database.EnsureCreated();

            foreach (var clientProvider in GetClientProviders())
                new AdventureWorksContext(clientProvider).Database.EnsureCreated();
        }

        public void OutputCurrentState(string subCategory = null)
        {
            var t = string.IsNullOrEmpty(subCategory) ? "" : $" - {subCategory}";

            //var methodParameters = this.Test.TestCase.Method.GetParameters().ToList();
            //var parameters = new StringBuilder();
            //if (methodParameters != null && methodParameters.Count > 0)
            //    foreach (ReflectionParameterInfo methodParameter in methodParameters)
            //        parameters.Append($"{methodParameter.Name}:{methodParameter.ParameterInfo.DefaultValue}. ");

            var overallTime = $"[Overall :{Fixture.OverallStopwatch.Elapsed.Minutes}:{Fixture.OverallStopwatch.Elapsed.Seconds}.{Fixture.OverallStopwatch.Elapsed.Milliseconds}]";
            var preparationTime = $"[Prework :{this.initializeStopwatch.Elapsed.Minutes}:{this.initializeStopwatch.Elapsed.Seconds}.{this.initializeStopwatch.Elapsed.Milliseconds}]";
            var testClass = this.Test.TestCase.TestMethod.TestClass.Class as ReflectionTypeInfo;

            t = $"{testClass.Type.Name}.{this.Test.TestCase.Method.Name}{t}: {overallTime} - {preparationTime} - {this.Stopwatch.Elapsed.Minutes}:{this.Stopwatch.Elapsed.Seconds}.{this.Stopwatch.Elapsed.Milliseconds}.";
            Console.WriteLine(t);
            Debug.WriteLine(t);
            this.Output.WriteLine(t);
        }

        public void Dispose()
        {

            this.Stopwatch.Stop();

            OutputCurrentState();
        }

    }
}
