using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using System;
using System.Threading.Tasks;

namespace WebSyncClient
{
    public static class SyncServices
    {
        public static async Task SynchronizeDefaultAsync(WebClientOrchestrator serverOrchestrator, SqlSyncProvider clientProvider, SyncOptions options, bool reinitialize = false)
        {
            var agent = new SyncAgent(clientProvider, serverOrchestrator, options);
            agent.Parameters.Add("City", "Toronto");
            agent.Parameters.Add("postal", DBNull.Value);
            var syncType = reinitialize ? SyncType.Reinitialize : SyncType.Normal;

            var progress = new SynchronousProgress<ProgressArgs>(
               pa => Console.WriteLine($"{pa.ProgressPercentage:p}\t {pa.Message}"));

            try
            {
                // Launch the sync process
                var s1 = await agent.SynchronizeAsync(syncType, progress).ConfigureAwait(false);

                // Write results
                Console.WriteLine(s1);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public static async Task SynchronizeLogsAsync(WebClientOrchestrator serverOrchestrator, SqlSyncProvider clientProvider, SyncOptions options, bool reinitialize = false)
        {
            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverOrchestrator, options, "logs");

            var progress = new SynchronousProgress<ProgressArgs>(
               pa => Console.WriteLine($"{pa.ProgressPercentage:p}\t {pa.Message}"));

            var syncType = reinitialize ? SyncType.Reinitialize : SyncType.Normal;

            try
            {
                // Launch the sync process
                var s1 = await agent.SynchronizeAsync(syncType, progress).ConfigureAwait(false);

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