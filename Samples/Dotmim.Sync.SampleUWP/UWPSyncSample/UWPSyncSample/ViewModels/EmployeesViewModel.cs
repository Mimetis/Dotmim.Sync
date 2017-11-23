using Dotmim.Sync;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Microsoft.Toolkit.Uwp.Helpers;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
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

        public ObservableCollection<EmployeeModel> Employees { get; set; }
        public ObservableCollection<String> Steps { get; set; }

       
        public EmployeesViewModel(INavigationService navigationService,
                                  IContosoServices contosoServices,
                                  SyncHelper syncHelper)
        {
            this.navigationService = navigationService;
            this.contosoServices = contosoServices;
            this.syncHelper = syncHelper;
            this.Employees = new ObservableCollection<EmployeeModel>();
            this.Steps = new ObservableCollection<string>();
        }

        public override async Task Navigated(NavigationEventArgs e, CancellationToken cancellationToken)
        {
            try
            {
                await RefreshAsync();
            }
            catch (Exception ex)
            {
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


        public async void SynchronizeClick(object sender, RoutedEventArgs e)
        {
            try
            {
                IsSynchronizing = true;
                Steps.Clear();

                var agent = this.syncHelper.GetSyncAgent();


                agent.SyncProgress += SyncProgress;
                var s = await agent.SynchronizeAsync();

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
