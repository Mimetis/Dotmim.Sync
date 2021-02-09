using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xamarin.Forms;
using Xamarin.Forms.Xaml;
using XamSyncSample.Models;
using XamSyncSample.ViewModels;
using XamSyncSample.Views;

namespace XamSyncSample.Views
{
    public partial class EmployeesPage : ContentPage
    {
        EmployeesViewModel _viewModel;

        public EmployeesPage()
        {
            InitializeComponent();

            BindingContext = _viewModel = new EmployeesViewModel();
        }

        protected override void OnAppearing()
        {
            base.OnAppearing();
            _viewModel.OnAppearing();
        }
    }
}