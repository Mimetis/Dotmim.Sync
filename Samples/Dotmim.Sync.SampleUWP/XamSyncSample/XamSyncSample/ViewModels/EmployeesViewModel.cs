using System;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Xamarin.Essentials;
using Xamarin.Forms;

using XamSyncSample.Models;
using XamSyncSample.Views;

namespace XamSyncSample.ViewModels
{
    public class EmployeesViewModel : BaseViewModel
    {
        private EmployeeViewModel selectedEmployee;

        public ObservableCollection<EmployeeViewModel> Employees { get; }
        public Command LoadEmployeesCommand { get; }
        public Command AddEmployeeCommand { get; }
        public Command<EmployeeViewModel> EmployeeTapped { get; }

        public EmployeesViewModel()
        {
            Title = "Browse";
            Employees = new ObservableCollection<EmployeeViewModel>();
            LoadEmployeesCommand = new Command(async () => await ExecuteLoadEmployeesCommand());

            EmployeeTapped = new Command<EmployeeViewModel>(OnEmployeeSelected);

            AddEmployeeCommand = new Command(OnAddEmployee);
        }

        async Task ExecuteLoadEmployeesCommand()
        {
            IsBusy = true;

            try
            {
                Employees.Clear();
                var employees = await DataStore.GetEmployeesAsync(true);
                foreach (var employee in employees)
                {
                    var model = new EmployeeViewModel();
                    await model.FillEmployeeAsync(employee);
                    Employees.Add(model);
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex);
            }
            finally
            {
                IsBusy = false;
            }
        }

        public void OnAppearing()
        {
            IsBusy = true;
            SelectedEmployee = null;
        }

        public EmployeeViewModel SelectedEmployee
        {
            get => selectedEmployee;
            set
            {
                SetProperty(ref selectedEmployee, value);
                OnEmployeeSelected(value);
            }
        }

        private async void OnAddEmployee(object obj)
        {
            await Shell.Current.GoToAsync(nameof(NewEmployeePage));
        }

        async void OnEmployeeSelected(EmployeeViewModel employee)
        {
            if (employee == null)
                return;

            // This will push the EmployeeDetailPage onto the navigation stack
            var d = $"{nameof(EmployeeDetailPage)}?{nameof(EmployeeViewModel.EmployeeId)}={employee.EmployeeId}";
            var e = $"{nameof(EmployeeDetailPage)}?RequestId={employee.EmployeeId}";

            await Shell.Current.GoToAsync(e);
        }
    }
}