using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using Microsoft.Data.Sqlite;
using System;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Windows;
using System.Windows.Controls;

namespace ClientWpf
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();
        }

        private async void Button_Click(object sender, RoutedEventArgs e)
        {
            if (cbCreateShapshot.IsChecked ?? false)
            {
                using (var httpClient = new HttpClient())
                {
                    using (var response = await httpClient.PostAsync("https://localhost:44321/snapshot", null))
                    {
                        response.EnsureSuccessStatusCode();
                    }
                }
            }

            var serverOrchestrator = new WebClientOrchestrator("https://localhost:44321/sync");
            var database = Path.Combine(Environment.CurrentDirectory, "database5.sqlite");
            File.Delete(database);
            var builder = new SqliteConnectionStringBuilder { DataSource = database };
            //var clientProvider = new SqliteSyncProvider(builder);

            var clientProvider = new SqlSyncProvider(@"Data Source=(localdb)\mssqllocaldb; Initial Catalog=Client; Integrated Security=true;");
           
            var agent = new SyncAgent(clientProvider, serverOrchestrator, new SyncOptions() { UseVerboseErrors = true });

            var progress = new Progress<ProgressArgs>(x => Debug.WriteLine($"{x.Context.SyncStage}: {x.Message} ({x.Hint})"));

            var b = sender as Button;
            b.IsEnabled = false;
            var s1 = await agent.SynchronizeAsync(
                SyncType.Reinitialize,
                default,
                progress);
            b.IsEnabled = true;

            MessageBox.Show(s1.ToString());
        }
    }
}
