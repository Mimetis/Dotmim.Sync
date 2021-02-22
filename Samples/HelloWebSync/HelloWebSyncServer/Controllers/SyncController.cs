using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Caching.Memory;

namespace HelloWebSyncServer.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class SyncController : ControllerBase
    {
        // The WebServerManager instance is useful to manage all the Web server orchestrators register in the Startup.cs
        private WebServerManager webServerManager;

        // Injected thanks to Dependency Injection
        public SyncController(WebServerManager webServerManager) => this.webServerManager = webServerManager;

        /// <summary>
        /// This POST handler is mandatory to handle all the sync process
        /// </summary>
        /// <returns></returns>
        [HttpPost]
        public async Task Post()
        {
            WebServerOrchestrator.TryGetHeaderValue(this.HttpContext.Request.Headers, "dotmim-sync-session-id", out var sessionId);

            var orchestrator = webServerManager.GetOrchestrator(this.HttpContext);


            // try get session cache from current sessionId
            if (webServerManager.Cache.TryGetValue<SessionCache>(sessionId, out var sessionCache))
            {
                WebServerOrchestrator.TryGetHeaderValue(this.HttpContext.Request.Headers, "dotmim-sync-step", out string iStep);

                var step = (HttpStep)Convert.ToInt32(iStep);

                if (step == HttpStep.GetMoreChanges)
                {
                    var random = new Random().Next(100);
                    if (random > 50)
                        throw new TimeoutException("Error waiting");
                }

            }

            await webServerManager.HandleRequestAsync(this.HttpContext);
        }

        /// <summary>
        /// This GET handler is optional. It allows you to see the configuration hosted on the server
        /// The configuration is shown only if Environmenent == Development
        /// </summary>
        [HttpGet]
        public async Task Get() => await webServerManager.HandleRequestAsync(this.HttpContext);
    }
}
