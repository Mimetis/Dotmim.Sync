using Dotmim.Sync;
using Dotmim.Sync.MySql;
using Dotmim.Sync.PostgreSql;
using Dotmim.Sync.PostgreSql.Scope;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using Npgsql;
using System;
using System.Net.Http;
using System.Threading.Tasks;

namespace HelloWebSyncClient
{
    class Program
    {
        //private static string clientConnectionString = $"Server=127.0.0.1;Port=3306;Database=Client;Uid=root;Pwd=Server123;";
        //private static string clientConnectionString = $"Host=localhost;Username=postgres;Password=postgres;Database=Offline;port=5433;Include Error Detail=true;";
        private static string clientConnectionString = $"Host=localhost;Username=postgres;Password=03095165265JMMtech@;Database=offline;Include Error Detail=true;";

        static async Task Main(string[] args)
        {

            //var obj = new NpgsqlScopeBuilder("hr.scope_info");
            //var connection = new NpgsqlConnection(clientConnectionString);
            //connection.Open();
            //var transaction = connection.BeginTransaction();
            //var command = obj.GetAllScopeInfoClientsCommand(connection, transaction);
            
            Console.WriteLine("Be sure the web api has started. Then click enter..");
            Console.ReadLine();
            await SynchronizeAsync();
        }

        private static async Task SynchronizeAsync()
        {
            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql 

            var serverOrchestrator = new WebRemoteOrchestrator("https://localhost:44342/api/sync");

            // Second provider is using plain old Sql Server provider, relying on triggers and tracking tables to create the sync environment
            //var clientProvider = new SqlSyncProvider(clientConnectionString);
            var clientProvider = new NpgsqlSyncProvider(clientConnectionString);
            //var clientProvider = new MySqlSyncProvider(clientConnectionString);

            var options = new SyncOptions
            {
                BatchSize = 1000
            };

            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverOrchestrator, options);

            do
            {
                try
                {
                    var progress = new SynchronousProgress<ProgressArgs>(args => Console.WriteLine($"{args.ProgressPercentage:p}:\t{args.Message}"));

                    // Launch the sync process
                    var s1 = await agent.SynchronizeAsync(progress);
                    // Write results
                    Console.WriteLine(s1);

                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }

            } while (Console.ReadKey().Key != ConsoleKey.Escape);

            Console.WriteLine("End");
        }
    }
}
