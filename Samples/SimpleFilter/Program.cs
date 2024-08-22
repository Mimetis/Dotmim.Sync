using Dotmim.Sync;
using Dotmim.Sync.SqlServer;
using System;
using System.Threading.Tasks;

namespace Filter
{
    internal class Program
    {
        private static string serverConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=AdventureWorks;Integrated Security=true;";
        private static string clientConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=Client;Integrated Security=true;";

        private static async Task Main() => await SynchronizeAsync().ConfigureAwait(false);

        private static async Task SynchronizeAsync()
        {
            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql
            var serverProvider = new SqlSyncProvider(serverConnectionString);
            var clientProvider = new SqlSyncProvider(clientConnectionString);

            var setup = new SyncSetup("ProductCategory", "Product");

            // Shortcut to create a filter directly from your SyncSetup instance
            // We are filtering all the product categories, by the ID (a GUID)
            setup.Filters.Add("ProductCategory", "ProductCategoryID");

            // For the second table (Product) We can also create the filter manually.
            // The next 4 lines are equivalent to : setup.Filters.Add("Product", "ProductCategoryID");
            var productFilter = new SetupFilter("Product");

            // Add a column as parameter. This column will be automaticaly added in the tracking table
            productFilter.AddParameter("ProductCategoryID", "Product");

            // add the side where expression, mapping the parameter to the column
            productFilter.AddWhere("ProductCategoryID", "Product", "ProductCategoryID");

            // add this filter to setup
            setup.Filters.Add(productFilter);

            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverProvider);
            var parameters = new SyncParameters(("ProductCategoryID", new Guid("10A7C342-CA82-48D4-8A38-46A2EB089B74")));
            do
            {
                // Launch the sync process
                var s1 = await agent.SynchronizeAsync(setup, parameters).ConfigureAwait(false);

                // Write results
                Console.WriteLine(s1);
            }
            while (Console.ReadKey().Key != ConsoleKey.Escape);

            Console.WriteLine("End");
        }
    }
}