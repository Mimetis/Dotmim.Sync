using System;
using Xamarin.Forms;
using Xamarin.Forms.Xaml;
using XamSyncSample.Context;
using XamSyncSample.Services;
using XamSyncSample.Views;

namespace XamSyncSample
{
    public partial class App : Application
    {

        public App()
        {
            InitializeComponent();

            DependencyService.Register<ContosoContext>();
            DependencyService.Register<SettingServices>();
            DependencyService.Register<SyncServices>();
            MainPage = new AppShell();
        }

        protected override void OnStart()
        {
        }

        protected override void OnSleep()
        {
        }

        protected override void OnResume()
        {
        }
    }
}
