using MauiAppClient.Services;

namespace MauiAppClient
{
    public partial class App : Application
    {
        public App()
        {
            InitializeComponent();

            SQLitePCL.Batteries_V2.Init();
            DependencyService.Register<SettingServices>();
            DependencyService.Register<SyncServices>();

            MainPage = new AppShell();
        }
    }
}