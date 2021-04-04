using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Xamarin.Forms;

namespace XamSyncSample.Services
{
    public class SettingServices : ISettingServices
    {

        private string GetLibraryPath()
        {
#if __IOS__
            // we need to put in /Library/ on iOS5.1 to meet Apple's iCloud terms
            // (they don't want non-user-generated data in Documents)
            string documentsPath = Environment.GetFolderPath(Environment.SpecialFolder.Personal); // Documents folder
            string libraryPath = Path.Combine(documentsPath, "..", "Library"); // Library folder instead
#else
            // Just use whatever directory SpecialFolder.Personal returns
            string libraryPath = Xamarin.Essentials.FileSystem.AppDataDirectory;
#endif

            return libraryPath;
        }

        public string DataSource => $"Data Source={DataSourcePath}";

        public string DataSourceName => "adv001.db";
        public string BatchDirectoryName => "dms";


        public string DataSourcePath => Path.Combine(GetLibraryPath(), DataSourceName);

        public string BatchDirectoryPath => Path.Combine(GetLibraryPath(), BatchDirectoryName);

        public string SyncApiUrl => Device.RuntimePlatform == Device.Android ? "https://10.0.2.2:44375/api/sync" : "https://localhost:44375/api/sync";

        public int BatchSize => 2000;
    }
}
