using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UWPSyncSample
{
    public enum ConnectionType
    {
        Client_SqlServer,
        Client_Sqlite,
        Client_MySql,
        WebProxy,
        Server_SqlServer,
        Server_MySql
    }
}
