using Dotmim.Sync;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.MySql;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.Web.Client;
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
        private SqliteSyncProvider sqliteSyncProvider;
        private MySqlSyncProvider mySqlSyncProvider;
        private WebProxyClientProvider webProxyProvider;

        string[] tables = new string[] { "Employees" };

        public SyncHelper(SettingsHelper settingsHelper)
        {
            this.settingsHelper = settingsHelper;
            this.contosoType = ConnectionType.Client_SqlServer;
            Init();
        }

        public SyncHelper(ConnectionType contosoType, SettingsHelper settingsHelper) : this(settingsHelper)
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

            sqliteSyncProvider = new SqliteSyncProvider(
                settingsHelper[ConnectionType.Client_Sqlite]);

            mySqlSyncProvider = new MySqlSyncProvider(
                settingsHelper[ConnectionType.Client_MySql]);

        }

        public SyncAgent GetSyncAgent(bool useHttp = false)
        {
            if (useHttp)
            {
                switch (this.contosoType)
                {
                    case ConnectionType.Client_SqlServer:
                        return new SyncAgent(sqlSyncProvider, webProxyProvider);
                    case ConnectionType.Client_Sqlite:
                        return new SyncAgent(sqliteSyncProvider, webProxyProvider);
                    case ConnectionType.Client_MySql:
                        return new SyncAgent(mySqlSyncProvider, webProxyProvider);
                }
            }
            else
            {
                switch (this.contosoType)
                {
                    case ConnectionType.Client_SqlServer:
                        return new SyncAgent(sqlSyncProvider, masterSqlSyncProvider, tables);
                    case ConnectionType.Client_Sqlite:
                        return new SyncAgent(sqliteSyncProvider, masterSqlSyncProvider, tables);
                    case ConnectionType.Client_MySql:
                        return new SyncAgent(mySqlSyncProvider, masterSqlSyncProvider, tables);
                }
            }

            return null;
        }

    }
}
