using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Web.Client;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Represent the arguments when the client is trying to send again an http request message.
    /// </summary>
    public class HttpSyncPolicyArgs : ProgressArgs
    {
        /// <inheritdoc cref="HttpSyncPolicyArgs" />
        public HttpSyncPolicyArgs(int retryCount, int retryNumber, TimeSpan delay, string host)
            : base(null, null, null)
        {
            this.RetryCount = retryCount;
            this.RetryNumber = retryNumber;
            this.Delay = delay;
            this.Host = host;
        }

        /// <inheritdoc />
        public override string Source => this.Host;

        /// <inheritdoc />
        public override string Message => $"Retry Sending Http Request ({this.RetryNumber}/{this.RetryCount}. Waiting {this.Delay.TotalMilliseconds}ms... )";

        /// <inheritdoc />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc />
        public override int EventId => HttpClientSyncEventsId.HttpSyncPolicy.Id;

        /// <summary>
        /// Gets the max number of retry for this http request.
        /// </summary>
        public int RetryCount { get; }

        /// <summary>
        /// Gets the current retry number.
        /// </summary>
        public int RetryNumber { get; }

        /// <summary>
        /// Gets the delay until next retry.
        /// </summary>
        public TimeSpan Delay { get; }

        /// <summary>
        /// Gets the host.
        /// </summary>
        public string Host { get; }
    }

    /// <summary>
    /// HttpClientSyncEventsId.
    /// </summary>
    public partial class HttpClientSyncEventsId
    {
        /// <summary>
        /// Gets the event id for HttpSyncPolicy.
        /// </summary>
        public static EventId HttpSyncPolicy => new(20500, nameof(HttpSyncPolicy));
    }

    /// <summary>
    /// Partial Interceptors extensions.
    /// </summary>
    public partial class HttpInterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when client is trying to send again an http request message.
        /// </summary>
        public static Guid OnHttpPolicyRetrying(
            this WebRemoteOrchestrator orchestrator,
            Action<HttpSyncPolicyArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when client is trying to send again an http request message.
        /// </summary>
        public static Guid OnHttpPolicyRetrying(
            this WebRemoteOrchestrator orchestrator,
            Func<HttpSyncPolicyArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}