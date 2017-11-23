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
        Client_Http_SqlServer,
        Client_Http_Sqlite,
        Client_MySql,
        Client_Http_MySql,
        WebProxy,
        Server_SqlServer,
        Server_MySql
    }
}
