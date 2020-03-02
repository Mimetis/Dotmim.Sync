using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Caching.Memory;
using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync.Web.Server
{
    /// <summary>
    /// Contains all providers registered on the server side
    /// </summary>
    public class WebServerProperties : ICollection<WebServerOrchestrator>, IList<WebServerOrchestrator>
    {
        private List<WebServerOrchestrator> innerCollection = new List<WebServerOrchestrator>();
        public IMemoryCache Cache { get; }
        public IHostingEnvironment Environment { get; }

        public WebServerProperties(IMemoryCache cache, IHostingEnvironment env)
        {
            this.Cache = cache;
            this.Environment = env;
        }


        /// <summary>
        /// Habdle request
        /// </summary>
        public async Task HandleRequestAsync(HttpContext context, CancellationToken cancellationToken = default)
        {
            if (!WebServerOrchestrator.TryGetHeaderValue(context.Request.Headers, "dotmim-sync-scope-name", out var scopeName))
                throw new HttpHeaderMissingExceptiopn("dotmim-sync-scope-name");

            if (context.Request.Method.ToLowerInvariant() == "get")
            {
                await this.WriteHelloAsync(context, cancellationToken);
                return;
            }

            await this[scopeName].HandleRequestAsync(context, cancellationToken).ConfigureAwait(false);
        }

       

        /// <summary>
        /// Add a new WebServerOrchestrator to the collection of WebServerOrchestrator
        /// </summary>
        public void Add(WebServerOrchestrator wsp)
        {
            if (innerCollection.Any(st => st.Setup.ScopeName == wsp.Setup.ScopeName))
                throw new Exception($"Scope {wsp.Setup.ScopeName} already exists in the collection");

            innerCollection.Add(wsp);
        }


        /// <summary>
        /// Get a WebServerOrchestrator by its scope name
        /// </summary>
        public WebServerOrchestrator this[string scopeName]
        {
            get
            {
                if (string.IsNullOrEmpty(scopeName))
                    throw new ArgumentNullException("scopeName");

                var wsp = innerCollection.FirstOrDefault(c => c.Setup.ScopeName.Equals(scopeName, SyncGlobalization.DataSourceStringComparison));

                if (wsp == null)
                    throw new ArgumentNullException($"Scope name {scopeName} does not exists");

                return wsp;
            }
        }




        private async Task WriteHelloAsync(HttpContext context, CancellationToken cancellationToken)
        {
            var httpResponse = context.Response;
            var stringBuilder = new StringBuilder();

            stringBuilder.AppendLine("<H1>Sync Configuration:</H1>");

            if (this.Environment != null && this.Environment.IsDevelopment())
            {
                stringBuilder.AppendLine("<div>Web Server properties:</div>");
                stringBuilder.AppendLine("<script src='https://cdn.jsdelivr.net/gh/google/code-prettify@master/loader/run_prettify.js'></script>");

                foreach (var webOrchestrator in this)
                {
                    stringBuilder.AppendLine("<H2>Scope name</H2>");
                    stringBuilder.AppendLine(webOrchestrator.Schema.ScopeName);

                    var s = JsonConvert.SerializeObject(webOrchestrator.Options, Formatting.Indented);
                    stringBuilder.AppendLine("<H2>Options</H2>");
                    stringBuilder.AppendLine("<pre class='prettyprint'>");
                    stringBuilder.AppendLine(s);
                    stringBuilder.AppendLine("</pre>");

                    s = JsonConvert.SerializeObject(webOrchestrator.Schema, Formatting.Indented);
                    stringBuilder.AppendLine("<H2>Schema</H2>");
                    stringBuilder.AppendLine("<pre class='prettyprint'>");
                    stringBuilder.AppendLine(s);
                    stringBuilder.AppendLine("</pre>");

                }


            }
            else
            {
                stringBuilder.AppendLine("<div>Server is configured to Production mode. No options displayed.</div>");
            }

            await httpResponse.WriteAsync(stringBuilder.ToString(), cancellationToken);


        }


        public void Clear() => this.innerCollection.Clear();
        public WebServerOrchestrator this[int index] => innerCollection[index];
        public int Count => innerCollection.Count;
        public bool IsReadOnly => false;
        WebServerOrchestrator IList<WebServerOrchestrator>.this[int index] { get => this.innerCollection[index]; set => this.innerCollection[index] = value; }
        public bool Remove(WebServerOrchestrator item) => innerCollection.Remove(item);
        public bool Contains(WebServerOrchestrator item) => innerCollection.Any(st => st.Setup.ScopeName.Equals(item.Setup.ScopeName, SyncGlobalization.DataSourceStringComparison));
        public bool Contains(string scopeName) => innerCollection.Any(st => st.Setup.ScopeName.Equals(scopeName, SyncGlobalization.DataSourceStringComparison));
        public void CopyTo(WebServerOrchestrator[] array, int arrayIndex) => innerCollection.CopyTo(array, arrayIndex);
        public int IndexOf(WebServerOrchestrator item) => innerCollection.IndexOf(item);
        public void RemoveAt(int index) => innerCollection.RemoveAt(index);
        public override string ToString() => this.innerCollection.Count.ToString();
        public void Insert(int index, WebServerOrchestrator item) => this.innerCollection.Insert(index, item);
        public IEnumerator<WebServerOrchestrator> GetEnumerator() => innerCollection.GetEnumerator();
        IEnumerator<WebServerOrchestrator> IEnumerable<WebServerOrchestrator>.GetEnumerator() => this.innerCollection.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => this.innerCollection.GetEnumerator();

    }
}
