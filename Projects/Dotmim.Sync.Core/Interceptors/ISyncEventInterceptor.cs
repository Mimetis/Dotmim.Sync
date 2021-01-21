using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public interface ISyncInterceptor : IDisposable
    { 
    }

    public interface ISyncInterceptor<T> : ISyncInterceptor
    {
        Task RunAsync(T args, CancellationToken cancellationToken);
    }

    public interface ISyncInterceptor2 : ISyncInterceptor
    {
        Task RunAsync(object args, CancellationToken cancellationToken);
    }

}
