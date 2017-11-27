using Dotmim.Sync;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Microsoft.Toolkit.Uwp.Helpers;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using UWPSyncSample.Context;
using UWPSyncSample.Helpers;
using UWPSyncSample.Models;
using UWPSyncSample.Navigation;
using UWPSyncSample.Services;
using UWPSyncSample.Views;
using Windows.UI.Popups;
using Windows.UI.Xaml;
using Windows.UI.Xaml.Controls;
using Windows.UI.Xaml.Navigation;

namespace UWPSyncSample.ViewModels
{
    public class EmployeesViewModel : BaseViewModel
    {
        private readonly INavigationService navigationService;
        private readonly IContosoServices contosoServices;
        private readonly SyncHelper syncHelper;
        private bool isSynchronizing;
        private SyncDirection syncDirection;
        private bool useHttp;
        private bool overwrite;

        public ObservableCollection<EmployeeModel> Employees { get; set; }
        public ObservableCollection<String> Steps { get; set; }

        public ObservableCollection<SyncDirection> SyncDirections { get; set; }

        public EmployeesViewModel(INavigationService navigationService,
                                  IContosoServices contosoServices,
                                  SyncHelper syncHelper)
        {
            this.navigationService = navigationService;
            this.contosoServices = contosoServices;
            this.syncHelper = syncHelper;
            this.Employees = new ObservableCollection<EmployeeModel>();
            this.Steps = new ObservableCollection<string>();

            this.SyncDirections = new ObservableCollection<SyncDirection>
            {
                SyncDirection.Bidirectional,
                SyncDirection.DownloadOnly,
                SyncDirection.UploadOnly
            };

            this.SyncDirection = SyncDirection.Bidirectional;

        }

        public override async Task Navigated(NavigationEventArgs e, CancellationToken cancellationToken)
        {
            try
            {
                await RefreshAsync();
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error : {ex}");

                MessageDialog cd = new MessageDialog("Cant make a database request. Try to sync");
                await cd.ShowAsync();
            }
        }

        public async Task RefreshAsync()
        {
            this.Employees.Clear();
            var employees = contosoServices.GetEmployees();

            if (employees == null)
                return;

            var lstEmployeesModel = new List<EmployeeModel>();

            // Double iteration for UI purpose (if not, it's clipping during animation)
            foreach (var emp in employees)
            {
                var em = new EmployeeModel(emp);
                await em.UpdatePictureAsync();
                lstEmployeesModel.Add(em);
            }

            foreach (var emp in lstEmployeesModel)
            {
                this.Employees.Add(emp);
            }
        }

        internal async void AddItemClick(object sender, RoutedEventArgs e)
        {
            await navigationService.NavigateToPage<EmployeeView>();
        }

        public async void EditClick(object sender, ItemClickEventArgs e)
        {
            await navigationService.NavigateToPage<EmployeeView>(e.ClickedItem);
        }

        public async void Reinitialize(object sender, RoutedEventArgs e)
        {
            await Synchronize(SyncType.Reinitialize);

        }

        public async void ReinitializeWithUpload(object sender, RoutedEventArgs e)
        {
            await Synchronize(SyncType.ReinitializeWithUpload);

        }


        public async void SynchronizeClick(object sender, RoutedEventArgs e)
        {
            await Synchronize();
        }

        private async Task Synchronize(SyncType syncType = SyncType.Normal)
        {
            try
            {
                IsSynchronizing = true;
                Steps.Clear();

                var agent = this.syncHelper.GetSyncAgent(this.UseHttp);

                // all config are applied on server side if http is enabled
                if (!this.UseHttp)
                {
                    agent.Configuration.OverwriteConfiguration = this.Overwrite;

                    if (this.SyncDirection == SyncDirection.DownloadOnly || this.SyncDirection == SyncDirection.UploadOnly)
                    {
                        foreach (var t in agent.Configuration)
                        {
                            t.SyncDirection = this.SyncDirection;
                        }
                    }
                }

                agent.SyncProgress += SyncProgress;
                var s = await agent.SynchronizeAsync(syncType, CancellationToken.None);

                if (s.TotalChangesDownloaded > 0)
                    await RefreshAsync();

                agent.SyncProgress -= SyncProgress;

            }
            catch (Exception ex)
            {
                MessageDialog cd = new MessageDialog($"Can't synhronize: {ex.Message}");
                await cd.ShowAsync();
            }
            finally
            {
                IsSynchronizing = false;
            }

        }

        public Boolean UseHttp
        {
            get
            {
                return this.useHttp;
            }
            set
            {
                if (value != useHttp)
                {
                    this.useHttp = value;
                    RaisePropertyChanged(nameof(UseHttp));
                }
            }
        }

        public Boolean Overwrite
        {
            get
            {
                return this.overwrite;
            }
            set
            {
                if (value != overwrite)
                {
                    this.overwrite = value;
                    RaisePropertyChanged(nameof(Overwrite));
                }
            }
        }

        public SyncDirection SyncDirection
        {
            get
            {
                return this.syncDirection;
            }
            set
            {
                if (value != syncDirection)
                {
                    this.syncDirection = value;
                    RaisePropertyChanged(nameof(SyncDirection));
                }
            }
        }
        public Boolean IsSynchronizing
        {
            get
            {
                return this.isSynchronizing;
            }
            set
            {
                if (value != isSynchronizing)
                {
                    this.isSynchronizing = value;
                    RaisePropertyChanged(nameof(IsSynchronizing));
                }
            }
        }


        private void Output(string str)
        {
            DispatcherHelper.ExecuteOnUIThreadAsync(() => Steps.Add(str));
        }

        private void SyncProgress(object sender, SyncProgressEventArgs e)
        {
            var sessionId = e.Context.SessionId.ToString();

            switch (e.Context.SyncStage)
            {
                case SyncStage.BeginSession:
                    Output($"Begin Session.");
                    break;
                case SyncStage.EndSession:
                    Output($"End Session.");
                    break;
                case SyncStage.EnsureScopes:
                    Output($"Ensure Scopes");
                    break;
                case SyncStage.EnsureConfiguration:
                    Output($"Configuration readed. {e.Configuration.ScopeSet.Tables.Count} table(s) involved.");
                    break;
                case SyncStage.EnsureDatabase:
                    Output($"Ensure Database");
                    break;
                case SyncStage.SelectingChanges:
                    Output($"Selecting changes...");
                    break;
                case SyncStage.SelectedChanges:
                    Output($"Changes selected : {e.ChangesStatistics.TotalSelectedChanges}");
                    break;
                case SyncStage.ApplyingChanges:
                    Output($"Applying changes...");
                    break;
                case SyncStage.ApplyingInserts:
                    Output($"\tApplying Inserts : {e.ChangesStatistics.AppliedChanges.Where(ac => ac.State == DmRowState.Added).Sum(ac => ac.ChangesApplied) }");
                    break;
                case SyncStage.ApplyingDeletes:
                    Output($"\tApplying Deletes : {e.ChangesStatistics.AppliedChanges.Where(ac => ac.State == DmRowState.Deleted).Sum(ac => ac.ChangesApplied) }");
                    break;
                case SyncStage.ApplyingUpdates:
                    Output($"\tApplying Updates : {e.ChangesStatistics.AppliedChanges.Where(ac => ac.State == DmRowState.Modified).Sum(ac => ac.ChangesApplied) }");
                    break;
                case SyncStage.AppliedChanges:
                    Output($"Changes applied : {e.ChangesStatistics.TotalAppliedChanges}");
                    break;
                case SyncStage.WriteMetadata:
                    if (e.Scopes != null)
                    {
                        Output($"Writing Scopes : ");
                        e.Scopes.ForEach(sc => Output($"\t{sc.Id} synced at {sc.LastSync}. "));
                    }
                    break;
                case SyncStage.CleanupMetadata:
                    Output($"CleanupMetadata");
                    break;
            }

            Console.ResetColor();
        }


    }
}
