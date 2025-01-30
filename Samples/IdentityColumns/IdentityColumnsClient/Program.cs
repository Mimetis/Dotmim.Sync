using Dotmim.Sync;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;

namespace IdentityColumnsClient
{
    class Program
    {
        private static string clientConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=Client;Integrated Security=true;";

        static async Task Main(string[] args)
        {
            Console.WriteLine("Be sure the web api has started. Then click enter..");
            Console.ReadLine();

            var clientProvider1 = new SqlSyncProvider(clientConnectionString);
            var clientProvider2 = new SqliteSyncProvider("client2.db");
            var clientProvider3 = new SqliteSyncProvider("client3.db");

            await SynchronizeAsync(clientProvider1);
            await SynchronizeAsync(clientProvider2);
            await SynchronizeAsync(clientProvider3);
        }

        private static async Task SynchronizeAsync(CoreProvider clientProvider)
        {
            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql 

            var serverOrchestrator = new WebClientOrchestrator("https://localhost:44342/api/sync");


            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverOrchestrator);

            try
            {
                var progress = new SynchronousProgress<ProgressArgs>(args => Console.WriteLine($"{args.ProgressPercentage:p}:\t{args.Message}"));

                // Get the client scope
                var scope = await agent.LocalOrchestrator.GetClientScopeAsync();

                if (scope.IsNewScope)
                {
                    var client = new HttpClient();
                    var seedingsResponse = await client.GetAsync($"https://localhost:44342/api/sync/seedings/{scope.Id}");

                    seedingsResponse.EnsureSuccessStatusCode();

                    var seedingsResponseString = await seedingsResponse.Content.ReadAsStringAsync();

                    var seedings = JsonSerializer.Deserialize<List<Seeding>>(seedingsResponseString);

                    agent.LocalOrchestrator.OnTableCreating(async tca =>
                    {
                        var tableName = tca.TableName.Unquoted().ToString();
                        var schemaName = string.IsNullOrEmpty(tca.TableName.SchemaName) ? "dbo" : tca.TableName.SchemaName;

                        var seeding = seedings.FirstOrDefault(s => s.TableName == tableName && s.SchemaName == schemaName);
                        var id = tca.Table.GetPrimaryKeysColumns().ToList()[0];

                        if (seeding != null && id.IsAutoIncrement)
                        {
                            id.AutoIncrementSeed = seeding.Seed;
                            id.AutoIncrementStep = seeding.Step;
                        }

                        var newTableBuilder = agent.LocalOrchestrator.GetTableBuilder(tca.Table, agent.LocalOrchestrator.Setup);

                        var newCommand = await newTableBuilder.GetCreateTableCommandAsync(tca.Connection, tca.Transaction);

                        tca.Command.CommandText = newCommand.CommandText;
                    });

                }

                // Launch the sync process
                var s1 = await agent.SynchronizeAsync(progress);
                // Write results
                Console.WriteLine(s1);

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            Console.WriteLine("End");
        }
    }
}
