using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Mvc;

namespace Dotmim.Sync.SampleWebServer.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class SyncController : ControllerBase
    {
        private WebProxyServerOrchestrator webProxyServer;

        // Injected thanks to Dependency Injection
        public SyncController(WebProxyServerOrchestrator proxy) => this.webProxyServer = proxy;

        [HttpPost]
        public async Task Post() => await webProxyServer.HandleRequestAsync(this.HttpContext);
    }
}
