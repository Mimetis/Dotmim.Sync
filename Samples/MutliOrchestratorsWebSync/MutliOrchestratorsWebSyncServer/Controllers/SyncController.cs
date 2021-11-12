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
        private IEnumerable<WebServerOrchestrator> orchestrators;

        // Injected thanks to Dependency Injection
        public SyncController(IEnumerable<WebServerOrchestrator> webServerOrchestrators) 
            => this.orchestrators = webServerOrchestrators;

        /// <summary>
        /// This POST handler is mandatory to handle all the sync process
        /// </summary>
        /// <returns></returns>
        [HttpPost]
        public async Task Post()
        {
            var scopeName = WebServerOrchestrator.GetScopeName(HttpContext);

            var orchestrator = orchestrators.FirstOrDefault(c => c.ScopeName == scopeName);

            await orchestrator.HandleRequestAsync(HttpContext).ConfigureAwait(false);

        }

        /// <summary>
        /// This GET handler is optional. It allows you to see the configuration hosted on the server
        /// The configuration is shown only if Environmenent == Development
        /// </summary>
        [HttpGet]
        public Task Get() => WebServerOrchestrator.WriteHelloAsync(this.HttpContext, orchestrators);

    }
}
