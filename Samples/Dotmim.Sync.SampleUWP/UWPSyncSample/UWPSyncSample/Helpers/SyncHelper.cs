using Dotmim.Sync;
using Dotmim.Sync.MySql;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UWPSyncSample.Context;

namespace UWPSyncSample.Helpers
{
    public class SyncHelper
    {
        private readonly SettingsHelper settingsHelper;
        private ConnectionType contosoType;

        private SqlSyncProvider masterSqlSyncProvider;
        private SqlSyncProvider sqlSyncProvider;
        private SqlSyncProvider sqlSyncProviderHttp;
        private SqliteSyncProvider sqliteSyncProvider;
        private SqliteSyncProvider sqliteSyncProviderHttp;
        private MySqlSyncProvider mySqlSyncProvider;
        private MySqlSyncProvider mySqlSyncProviderHttp;
        private WebProxyClientProvider webProxyProvider;

        string[] tables = new string[] { "Employees" };

        public SyncHelper(SettingsHelper settingsHelper)
        {
            this.settingsHelper = settingsHelper;
            this.contosoType = ConnectionType.Client_SqlServer;
            Init();
        }

        public SyncHelper(ConnectionType contosoType, SettingsHelper settingsHelper) :this(settingsHelper)
        {
            this.contosoType = contosoType;
        }

        private void Init()
        {
            // Servers providers
            masterSqlSyncProvider = new SqlSyncProvider(
                settingsHelper[ConnectionType.Server_SqlServer]);

            webProxyProvider = new WebProxyClientProvider(
                 new Uri(settingsHelper[ConnectionType.WebProxy]));

            // clients providers
            sqlSyncProvider = new SqlSyncProvider(
                settingsHelper[ConnectionType.Client_SqlServer]);

            sqlSyncProviderHttp = new SqlSyncProvider(
                settingsHelper[ConnectionType.Client_Http_SqlServer]);

            sqliteSyncProvider = new SqliteSyncProvider(
                settingsHelper[ConnectionType.Client_Sqlite]);

            sqliteSyncProviderHttp = new SqliteSyncProvider(
                settingsHelper[ConnectionType.Client_Http_Sqlite]);

            mySqlSyncProvider = new MySqlSyncProvider(
                settingsHelper[ConnectionType.Client_MySql]);

            mySqlSyncProviderHttp = new MySqlSyncProvider(
                settingsHelper[ConnectionType.Client_Http_MySql]);

        }

        public SyncAgent GetSyncAgent()
        {
            switch (this.contosoType)
            {
                case ConnectionType.Client_SqlServer:
                    return new SyncAgent(sqlSyncProvider, masterSqlSyncProvider, tables);
                case ConnectionType.Client_Sqlite:
                    return new SyncAgent(sqliteSyncProvider, masterSqlSyncProvider, tables);
                case ConnectionType.Client_MySql:
                    return new SyncAgent(mySqlSyncProvider, masterSqlSyncProvider, tables);
                case ConnectionType.Client_Http_SqlServer:
                    return new SyncAgent(sqlSyncProviderHttp, webProxyProvider);
                case ConnectionType.Client_Http_Sqlite:
                    return new SyncAgent(sqliteSyncProviderHttp, webProxyProvider);
                case ConnectionType.Client_Http_MySql:
                    return new SyncAgent(mySqlSyncProviderHttp, webProxyProvider);
            }

            return null;
        }

    }
}
