using Dotmim.Sync;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Microsoft.Data.Sqlite;

namespace WinFormsCore
{
    public partial class Form1 : Form
    {
        private static string serverConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=AdventureWorks;Integrated Security=true;";
        private string dbName = "adv2.db";

        public Form1()
        {
            this.InitializeComponent();
        }

        private async void button1_Click(object sender, EventArgs e)
        {

            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql

            // Create 2 Sql Sync providers
            // First provider is using the Sql change tracking feature. Don't forget to enable it on your database until running this code !
            // For instance, use this SQL statement on your server database : ALTER DATABASE AdventureWorks  SET CHANGE_TRACKING = ON  (CHANGE_RETENTION = 10 DAYS, AUTO_CLEANUP = ON)
            // Otherwise, if you don't want to use Change Tracking feature, just change 'SqlSyncChangeTrackingProvider' to 'SqlSyncProvider'
            var serverProvider = new SqlSyncProvider(serverConnectionString);

            // Second provider is using plain old Sql Server provider, relying on triggers and tracking tables to create the sync environment
            var clientProvider = new SqliteSyncProvider(this.dbName);

            // Tables involved in the sync process:
            var setup = new SyncSetup("ProductCategory", "ProductModel", "Product",
                        "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail");

            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverProvider);

            // Launch the sync process
            var s1 = await agent.SynchronizeAsync(setup).ConfigureAwait(false);

            // Write results
            MessageBox.Show(s1.ToString());
        }

        private static void Clear(string dbName)
        {
            SqliteConnection.ClearAllPools();
            GC.Collect();
            GC.WaitForPendingFinalizers();
            var filePath = GetSqliteFilePath(dbName);

            if (File.Exists(filePath))
                File.Delete(filePath);
        }

        public static string GetSqliteFilePath(string dbName)
        {
            var fi = new FileInfo(dbName);

            if (string.IsNullOrEmpty(fi.Extension))
                dbName = $"{dbName}.db";

            return Path.Combine(Directory.GetCurrentDirectory(), dbName);
        }

        private void button2_Click(object sender, EventArgs e)
        {
            Clear(this.dbName);
        }
    }
}