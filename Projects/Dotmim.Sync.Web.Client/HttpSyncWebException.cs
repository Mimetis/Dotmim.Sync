using System.Net;

namespace Dotmim.Sync.Web.Client
{
    /// <summary>
    /// HttpSyncWebException is thrown when an HTTP request fails with a specific status code.
    /// </summary>
    public class HttpSyncWebException : SyncException
    {
        /// <summary>
        /// Gets or sets the reason phrase which typically is sent by servers together with the status code.
        /// </summary>
        public string ReasonPhrase { get; set; }

        /// <summary>
        /// Gets or sets the status code of the HTTP response.
        /// </summary>
        public HttpStatusCode StatusCode { get; set; }

        /// <inheritdoc cref="HttpSyncWebException" />
        public HttpSyncWebException(string message)
            : base(message)
        {
        }
    }
}