using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using MauiAppClient.Services;
using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MauiAppClient.ViewModels
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
        public Command CustomActionCommand { get; }

        public SyncViewModel()
        {
            try
            {

            this.Title = "Sync";
            this.SyncProgressionText = "Ready...";
            this.SyncCommandButtonEnabled = true;
            this.SyncCommand = new Command(async () => await ExecuteSyncCommand(SyncType.Normal));
            this.SyncReinitializeCommand = new Command(async () => await ExecuteSyncCommand(SyncType.Reinitialize));
            this.CustomActionCommand = new Command(() => ExecuteCustomActionCommand());

            this.syncAgent = DependencyService.Get<ISyncServices>().GetSyncAgent();
            this.settings = DependencyService.Get<ISettingServices>();

            this.syncAgent.SessionStateChanged += (s, state) =>
                this.SyncCommandButtonEnabled = state == SyncSessionState.Ready ? true : false;
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
            }
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

                var progress = new Progress<ProgressArgs>(args =>
                {
                    MainThread.BeginInvokeOnMainThread(() =>
                    {
                        this.SyncProgress = args.ProgressPercentage;
                        this.SyncProgressionText = args.Message;
                    });
                });

                var r = await this.syncAgent.SynchronizeAsync(syncType, progress);

                MainThread.BeginInvokeOnMainThread(() => this.SyncProgressionText = r.ToString());
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

        private void ExecuteCustomActionCommand()
        {
            IsBusy = true;

            try
            {
                CustomActionInsertProductRow();
                this.SyncProgressionText = "1000 Categories added";
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

        private void CustomActionInsertProductRow()
        {
            using var sqliteConnection = new SqliteConnection(this.settings.DataSource);
            var c = new SqliteCommand($"Insert into ProductCategory(ProductcategoryId, Name, rowguid, ModifiedDate) Values (@productcategoryId, @name, @rowguid, @modifiedDate)")
            {
                Connection = sqliteConnection
            };

            var p = new SqliteParameter
            {
                DbType = DbType.Guid,
                ParameterName = "@productcategoryId"
            };
            c.Parameters.Add(p);

            p = new SqliteParameter
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
                    c.Parameters[0].Value = Guid.NewGuid();
                    c.Parameters[1].Value = Guid.NewGuid().ToString();
                    c.Parameters[2].Value = Guid.NewGuid().ToString();
                    c.Parameters[3].Value = DateTime.Now;

                    c.ExecuteNonQuery();
                }

                t.Commit();
            }

            sqliteConnection.Close();

        }
        public void OnAppearing()
        {
            IsBusy = true;
        }
    }

}
