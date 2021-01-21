using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Tools
{
    public class Provider
    {
        public ProviderType ProviderType { get; set; }

        public String ConnectionString { get; set; }

        public SyncType SyncType { get; set; }
    }

    public enum SyncType
    {
        Server,
        Client
    }

    public enum ProviderType
    {
        SqlServer,
        Sqlite,
        MySql,
        Web
    }
}
