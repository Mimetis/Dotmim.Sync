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

namespace Dotmim.Sync.Tests
{
    public class DatabaseTest<T> : IDisposable where T : RelationalFixture
    {
        private Stopwatch initializeStopwatch;
        public virtual DatabaseServerFixture<T> Fixture { get; }
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

        public DatabaseTest(ITestOutputHelper output, DatabaseServerFixture<T> fixture)
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

            ResetClientsAndServerByCreatingThemAgain();

            initializeStopwatch.Stop();

            this.Stopwatch = Stopwatch.StartNew();
        }

        private void ResetClientsTables()
        {
            // Drop DMS metadatas and truncate clients tables
            foreach (var clientProvider in Fixture.GetClientProviders())
            {
                // drop all DMS tracking tables & metadatas
                Fixture.DropAllTablesAsync(clientProvider, false).GetAwaiter().GetResult();
                // truncate all tables
                Fixture.EmptyAllTablesAsync(clientProvider).GetAwaiter().GetResult();
            }
        }

        private void ResetClientsAndServerByCreatingThemAgain()
        {
            HelperDatabase.DropDatabase(Fixture.ServerProviderType, Fixture.ServerDatabaseName);
            new AdventureWorksContext(Fixture.ServerDatabaseName, Fixture.ServerProviderType, Fixture.UseFallbackSchema, true).Database.EnsureCreated();

            // Drop DMS metadatas and truncate clients tables
            foreach (var (clientType, clientDatabaseName) in Fixture.ClientDatabaseNames)
            {
                HelperDatabase.DropDatabase(clientType, clientDatabaseName);
                new AdventureWorksContext(clientDatabaseName, clientType, Fixture.UseFallbackSchema, false).Database.EnsureCreated();
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
