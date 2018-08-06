using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Web.Core
{
    /// <summary>
    /// Http steps involved during a sync beetween a proxy client and proxy server
    /// </summary>
    public enum HttpStep
    {
        BeginSession,
        EnsureScopes,
        EnsureConfiguration,
        EnsureDatabase,
        GetChangeBatch,
        ApplyChanges,
        WriteScopes,
        GetLocalTimestamp,
        EndSession
    }
}
