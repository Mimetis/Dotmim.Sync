using MauiAppClient.ViewModels;

namespace MauiAppClient
{
    public partial class MainPage : ContentPage
    {
        int count = 0;

        private SyncViewModel viewModel;

        public MainPage()
        {
            InitializeComponent();
            BindingContext = viewModel = new SyncViewModel();
        }

        protected override void OnAppearing()
        {
            base.OnAppearing();
            viewModel.OnAppearing();
        }
        
    }
}