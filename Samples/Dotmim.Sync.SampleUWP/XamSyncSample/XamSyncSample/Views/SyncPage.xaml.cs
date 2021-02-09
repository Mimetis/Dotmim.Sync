using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Xamarin.Forms;
using Xamarin.Forms.Xaml;
using XamSyncSample.ViewModels;

namespace XamSyncSample.Views
{
    [XamlCompilation(XamlCompilationOptions.Compile)]
    public partial class SyncPage : ContentPage
    {
        private SyncViewModel _viewModel;

        public SyncPage()
        {
            InitializeComponent();
            BindingContext = _viewModel = new SyncViewModel();
        }

        protected override void OnAppearing()
        {
            base.OnAppearing();
            _viewModel.OnAppearing();
        }
    }
}