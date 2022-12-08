using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace MauiAppClient.Services
{
    public class SettingServices : ISettingServices
    {

        private string GetLibraryPath()
        {
            //#if __IOS__
            //            // we need to put in /Library/ on iOS5.1 to meet Apple's iCloud terms
            //            // (they don't want non-user-generated data in Documents)
            //            string documentsPath = Environment.GetFolderPath(Environment.SpecialFolder.Personal); // Documents folder
            //            string libraryPath = Path.Combine(documentsPath, "..", "Library"); // Library folder instead
            //#else
            //            // Just use whatever directory SpecialFolder.Personal returns
            //            string libraryPath = FileSystem.AppDataDirectory;
            //#endif

            string libraryPath = FileSystem.AppDataDirectory;
            return libraryPath;
        }

        public string DataSource => $"Data Source={DataSourcePath}";

        public string DataSourceName => "adv0024.db";
        public string BatchDirectoryName => "dms";
        public string DataSourcePath => Path.Combine(GetLibraryPath(), DataSourceName);
        public string BatchDirectoryPath => Path.Combine(GetLibraryPath(), BatchDirectoryName);

        // Testing from emulator
        public string SyncApiUrl => DeviceInfo.Platform == DevicePlatform.Android ? "http://10.0.2.2:5213/api/sync" : "http://localhost:5213/api/sync";

        // Testing from a device with dev tunnels
        // More here : https://devblogs.microsoft.com/visualstudio/public-preview-of-dev-tunnels-in-visual-studio-for-asp-net-core-projects/
        //
        // public string SyncApiUrl => "https://073cr2sz-44318.uks1.devtunnels.ms/api/sync";

        public int BatchSize => 2000;
    }
}
