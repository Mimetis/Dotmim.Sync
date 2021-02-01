using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Web.Client;
using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Xamarin.Forms;
using XamSyncSample.Services;

namespace XamSyncSample.ViewModels
{

    public class SyncViewModel : BaseViewModel
    {
        private string syncProgressionText;
        private bool syncCommandButtonEnabled;
        private double syncProgress;


        /// <summary>
        /// Get a the Sync Agent instance
        /// </summary>
        private SyncAgent syncAgent;
        private ISettingServices settings;

        public Command SyncCommand { get; }
        public Command SyncReinitializeCommand { get; }
        public Command AddRowsCommand { get; }

        public SyncViewModel()
        {
            this.Title = "Browse";
            this.SyncProgressionText = "Ready...";
            this.SyncCommandButtonEnabled = true;
            this.SyncCommand = new Command(async () => await ExecuteSyncCommand(SyncType.Normal));
            this.SyncReinitializeCommand = new Command(async () => await ExecuteSyncCommand(SyncType.Reinitialize));
            this.AddRowsCommand = new Command(() => ExecuteAddRowsCommand());

            this.syncAgent = DependencyService.Get<ISyncServices>().GetSyncAgent();
            this.settings = DependencyService.Get<ISettingServices>();

            this.syncAgent.SessionStateChanged += (s, state) =>
                this.SyncCommandButtonEnabled = state == SyncSessionState.Ready ? true : false;
        }

        public string SyncProgressionText
        {
            get => syncProgressionText;
            set => SetProperty(ref syncProgressionText, value);
        }

        public bool SyncCommandButtonEnabled
        {
            get => syncCommandButtonEnabled;
            set => SetProperty(ref syncCommandButtonEnabled, value);
        }
        public double SyncProgress
        {
            get => syncProgress;
            set => SetProperty(ref syncProgress, value);
        }

        private async Task ExecuteSyncCommand(SyncType syncType)
        {
            IsBusy = true;

            try
            {
                var progress = new SynchronousProgress<ProgressArgs>(args =>
                {
                    Device.BeginInvokeOnMainThread(() =>
                    {
                        this.SyncProgress = args.PogressPercentage;
                        this.SyncProgressionText = args.Message;
                    });
                });

                var r = await this.syncAgent.SynchronizeAsync(syncType, progress);

                Device.BeginInvokeOnMainThread(() => this.SyncProgressionText = r.ToString());
            }
            catch (Exception ex)
            {
                SyncProgressionText = ex.Message;
            }
            finally
            {
                IsBusy = false;
            }
        }

        private void ExecuteAddRowsCommand()
        {
            IsBusy = true;

            try
            {
                using (var sqliteConnection = new SqliteConnection(this.settings.DataSource))
                {
                    var c = new SqliteCommand($"Insert into ProductCategory(Name, rowguid, ModifiedDate) Values (@name, @rowguid, @modifiedDate)")
                    {
                        Connection = sqliteConnection
                    };

                    var p = new SqliteParameter
                    {
                        DbType = DbType.String,
                        Size = 50,
                        ParameterName = "@name"
                    };
                    c.Parameters.Add(p);

                    p = new SqliteParameter
                    {
                        DbType = DbType.String,
                        Size = 36,
                        ParameterName = "@rowguid"
                    };
                    c.Parameters.Add(p);

                    p = new SqliteParameter
                    {
                        DbType = DbType.DateTime,
                        ParameterName = "@modifiedDate"
                    };
                    c.Parameters.Add(p);

                    sqliteConnection.Open();

                    c.Prepare();

                    using (var t = sqliteConnection.BeginTransaction())
                    {

                        for (var i = 0; i < 10000; i++)
                        {
                            c.Transaction = t;
                            c.Parameters[0].Value = Guid.NewGuid().ToString();
                            c.Parameters[1].Value = Guid.NewGuid().ToString();
                            c.Parameters[2].Value = DateTime.Now;

                            c.ExecuteNonQuery();
                        }

                        t.Commit();
                    }

                    sqliteConnection.Close();
                }
            }
            catch (Exception ex)
            {
                SyncProgressionText = ex.Message;
            }
            finally
            {
                IsBusy = false;
            }
        }


        public void OnAppearing()
        {
            IsBusy = true;
        }
    }


}
