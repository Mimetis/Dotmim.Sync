using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using UWPSyncSample.Context;
using UWPSyncSample.Helpers;
using UWPSyncSample.Models;
using UWPSyncSample.Navigation;
using UWPSyncSample.Services;
using UWPSyncSample.Views;
using Windows.Storage;
using Windows.Storage.Pickers;
using Windows.Storage.Streams;
using Windows.UI.Xaml;
using Windows.UI.Xaml.Media.Imaging;
using Windows.UI.Xaml.Navigation;

namespace UWPSyncSample.ViewModels
{
    public class EmployeeViewModel : BaseViewModel
    {
        private EmployeeModel employeeModel;
        private bool isNew;
        private readonly INavigationService navigationService;
        private readonly IContosoServices contosoServices;

        public EmployeeModel EmployeeModel
        {
            get
            {
                return employeeModel;
            }
            private set
            {
                if (value != employeeModel)
                {
                    employeeModel = value;
                    RaisePropertyChanged(nameof(EmployeeModel));
                }
            }
        }

        public Boolean IsNew
        {
            get
            {
                return isNew;
            }
            private set
            {
                if (value != isNew)
                {
                    isNew = value;
                    RaisePropertyChanged(nameof(IsNew));
                }
            }
        }

        public EmployeeViewModel(INavigationService navigationService, IContosoServices contosoServices)
        {
            this.navigationService = navigationService;
            this.contosoServices = contosoServices;
        }

        public override Task Navigated(NavigationEventArgs e, CancellationToken cancellationToken)
        {
            this.IsNew = e.Parameter != null;

            this.EmployeeModel = !this.IsNew ? EmployeeModel.NewEmployee() : e.Parameter as EmployeeModel;

            return Task.CompletedTask;
        }

        public async void SaveClick(object sender, RoutedEventArgs e)
        {
            contosoServices.SaveEmployee(this.EmployeeModel.GetEmployee());

            await navigationService.NavigateToPage<EmployeesView>();

        }

        public async void DeleteClick(object sender, RoutedEventArgs e)
        {
            contosoServices.DeleteEmployee(this.EmployeeModel.EmployeeId);

            await navigationService.NavigateToPage<EmployeesView>();

        }

        public async void BrowseClick(object sender, RoutedEventArgs e)
        {
            FileOpenPicker open = new FileOpenPicker();
            open.SuggestedStartLocation = PickerLocationId.PicturesLibrary;
            open.ViewMode = PickerViewMode.Thumbnail;

            // Filter to include a sample subset of file types
            open.FileTypeFilter.Clear();
            open.FileTypeFilter.Add(".bmp");
            open.FileTypeFilter.Add(".png");
            open.FileTypeFilter.Add(".jpeg");
            open.FileTypeFilter.Add(".jpg");

            // Open a stream for the selected file
            StorageFile file = await open.PickSingleFileAsync();

            // Ensure a file was selected
            if (file != null)
            {
                EmployeeModel.ProfilePicture = await ImageHelper.Current.ToByteArrayAsync(file);
                employeeModel.Picture = await ImageHelper.Current.FromArrayByteAsync(EmployeeModel.ProfilePicture);
            }
        }

    }
}

