using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Core.Proxy.Client
{
    public enum HttpStep
    {
        BeginSession,
        EnsureScopes,
        ApplyConfiguration,
        GetConfiguration,
        EnsureDatabase,
        ApplyChanges,
        GetChangeBatch,
        WriteScopes,
        EndSession,
        GetLocalTimestamp

    }
}
