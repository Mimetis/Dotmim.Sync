using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Mvc;
using UWPSyncSampleWebServer.Context;

namespace UWPSyncSampleWebServer.Controllers
{
    [Route("api/[controller]")]
    public class ValuesController : Controller
    {

        private WebProxyServerOrchestrator webProxyServer;

        // Injected thanks to Dependency Injection
        public ValuesController(WebProxyServerOrchestrator proxy) => this.webProxyServer = proxy;

         // POST api/values
        [HttpPost]
        public async Task Post()
        {
 
            await webProxyServer.HandleRequestAsync(this.HttpContext);
        }

    }
}
