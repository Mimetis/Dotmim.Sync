using Dotmim.Sync.Args;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class BaseOrchestrator
    {
        internal IScopeInfo InternalCreateScopeInfo(string scopeName, DbScopeType scopeType)
        {
            // create a new scope id for the current owner (could be server or client as well)
            IScopeInfo scope = scopeType switch
            {
                DbScopeType.Client => new ClientScopeInfo { Id = Guid.NewGuid(), Name = scopeName, IsNewScope = true, LastSync = null, Version = SyncVersion.Current.ToString() },
                DbScopeType.Server => new ServerScopeInfo { Name = scopeName, LastCleanupTimestamp = 0, IsNewScope = true, Version = SyncVersion.Current.ToString() },
                _ => throw new NotImplementedException($"Type of scope {scopeName} is not implemented when trying to get a single instance")
            };

            return scope;
        }
    }
}
