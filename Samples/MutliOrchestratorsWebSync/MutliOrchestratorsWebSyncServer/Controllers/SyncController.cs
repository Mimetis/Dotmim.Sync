using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Caching.Memory;
using Dotmim.Sync;

namespace MutliOrchestratorsWebSyncServer.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class SyncController : ControllerBase
    {
        public IEnumerable<WebServerOrchestrator> WebServerOrchestrators { get; }

        // Injected thanks to Dependency Injection
        public SyncController(IEnumerable<WebServerOrchestrator> webServerOrchestrators) => this.WebServerOrchestrators = webServerOrchestrators;

        /// <summary>
        /// This POST handler is mandatory to handle all the sync process
        /// </summary>
        /// <returns></returns>
        [HttpPost]
        public async Task Post()
        {
            if (!WebServerOrchestrator.TryGetHeaderValue(HttpContext.Request.Headers, "dotmim-sync-scope-name", out var scopeName))
                throw new HttpHeaderMissingException("dotmim-sync-scope-name");

            var orchestrator = WebServerOrchestrators.FirstOrDefault(c => c.ScopeName.Equals(scopeName, SyncGlobalization.DataSourceStringComparison));

            await orchestrator.HandleRequestAsync(HttpContext).ConfigureAwait(false);

        }

        /// <summary>
        /// This GET handler is optional. It allows you to see the configuration hosted on the server
        /// The configuration is shown only if Environmenent == Development
        /// </summary>
        [HttpGet]
        public Task Get() => WebServerManager.WriteHelloAsync(this.HttpContext, WebServerOrchestrators);

    }
}
