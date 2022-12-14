using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Web.Client;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public class HttpSyncPolicyArgs : ProgressArgs
    {
        public HttpSyncPolicyArgs(int retryCount, int retryNumber, TimeSpan delay, string host)
            : base(null, null, null)
        {
            this.RetryCount = retryCount;
            this.RetryNumber = retryNumber;
            this.Delay = delay;
            this.Host = host;
        }
        public override string Source => this.Host;

        public override string Message => $"Retry Sending Http Request ({RetryNumber}/{RetryCount}. Waiting {Delay.TotalMilliseconds}ms... )";
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;
        public override int EventId => HttpClientSyncEventsId.HttpSyncPolicy.Id;

        /// <summary>
        /// Gets the max number of retry for this http request
        /// </summary>
        public int RetryCount { get; }

        /// <summary>
        /// Gets the current retry number
        /// </summary>
        public int RetryNumber { get; }

        /// <summary>
        /// Gets the delay until next retry
        /// </summary>
        public TimeSpan Delay { get; }
        public string Host { get; }
    }

    public static partial class HttpClientSyncEventsId
    {
        public static EventId HttpSyncPolicy => new EventId(20500, nameof(HttpSyncPolicy));
    }

    /// <summary>
    /// Partial interceptors extensions 
    /// </summary>
    public static partial class HttpInterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when client is trying to send again an http request message 
        /// </summary>
        public static Guid OnHttpPolicyRetrying(this WebRemoteOrchestrator orchestrator,
            Action<HttpSyncPolicyArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when client is trying to send again an http request message 
        /// </summary>
        public static Guid OnHttpPolicyRetrying(this WebRemoteOrchestrator orchestrator,
            Func<HttpSyncPolicyArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
    }
