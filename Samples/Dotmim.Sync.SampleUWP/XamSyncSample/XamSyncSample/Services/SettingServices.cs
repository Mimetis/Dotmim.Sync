using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace XamSyncSample.Services
{
    public class SettingServices : ISettingServices
    {


        public string DataSource
        {
            get
            {
                var datasource = $"Data Source={DataSourcePath}";

                return datasource;

            }
        }

        public string DataSourceName => "Adv.db";


        public string DataSourcePath
        {
            get
            {
#if __IOS__
                // we need to put in /Library/ on iOS5.1 to meet Apple's iCloud terms
                // (they don't want non-user-generated data in Documents)
                string documentsPath = Environment.GetFolderPath(Environment.SpecialFolder.Personal); // Documents folder
                string libraryPath = Path.Combine(documentsPath, "..", "Library"); // Library folder instead
#else
                // Just use whatever directory SpecialFolder.Personal returns
                string libraryPath = Environment.GetFolderPath(Environment.SpecialFolder.Personal); ;
#endif
                var path = Path.Combine(libraryPath, DataSourceName);
                return path;
            }
        }

        public string SyncApiUrl
        {
            get
            {
                return "http://10.0.2.2:54347/api/sync";

            }
        }

        public int SyncBatchSize
        {
            get
            {
                return 1000;

            }
        }
    }
}
