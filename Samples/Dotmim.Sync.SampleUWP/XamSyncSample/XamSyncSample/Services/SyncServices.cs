using Dotmim.Sync;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.Web.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xamarin.Essentials;
using Xamarin.Forms;

namespace XamSyncSample.Services
{
    public class SyncServices : ISyncServices
    {

        private SqliteSyncProvider sqliteSyncProvider;
        private SyncAgent syncAgent;
        private WebClientOrchestrator webProxyProvider;

        private ISettingServices settings;

        public SyncServices()
        {
            this.settings = DependencyService.Get<ISettingServices>();
            this.webProxyProvider = new WebClientOrchestrator(this.settings.SyncApiUrl);
            this.sqliteSyncProvider = new SqliteSyncProvider(this.settings.DataSource);
            var clientOptions = new SyncOptions { BatchSize = settings.SyncBatchSize };
            this.syncAgent = new SyncAgent(sqliteSyncProvider, webProxyProvider, clientOptions);
        }

        public SyncAgent GetSyncAgent() => this.syncAgent;

    }
}
