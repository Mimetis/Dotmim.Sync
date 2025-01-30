using System;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Interceptor interface.
    /// </summary>
    public interface ISyncInterceptor : IDisposable
    {
        /// <summary>
        /// Gets the Interceptor Id.
        /// </summary>
        Guid Id { get; }
    }

    /// <summary>
    /// Interceptor interface of T.
    /// </summary>
    public interface ISyncInterceptor<T> : ISyncInterceptor
    {

        /// <summary>
        /// Run the Action or Func as the Interceptor.
        /// </summary>
        Task RunAsync(T args, CancellationToken cancellationToken);
    }
}