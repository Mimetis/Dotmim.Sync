using Dotmim.Sync;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.Web.Client;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xamarin.Forms;

namespace XamAppSync
{
    public partial class MainPage : ContentPage
    {
        public MainPage()
        {
            InitializeComponent();
        }

        private async void Handle_Clicked(object sender, System.EventArgs e)
        {
            try
            {


                // Use this when you want to use sqlite on ios
                SQLitePCL.Batteries_V2.Init();

                // 10.0.2.2 is a special url to be able to reach the machine localhost web api
                var serverOrchestrator = new WebClientOrchestrator("http://10.0.2.2:50886/api/sync");

                // Path the local sqlite database
                string dbPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Personal), "client_sync.db");

                // Sqlite Client provider
                var clientProvider = new SqliteSyncProvider(dbPath);

                // Creating an agent that will handle all the process
                var agent = new SyncAgent(clientProvider, serverOrchestrator);

                // Launch the sync process
                var s1 = await agent.SynchronizeAsync();

                // See results
                lblResult.Text = s1.ToString();
            }
            catch (Exception ex)
            {
                lblResult.Text = ex.Message;
            }
        }
    }
}
