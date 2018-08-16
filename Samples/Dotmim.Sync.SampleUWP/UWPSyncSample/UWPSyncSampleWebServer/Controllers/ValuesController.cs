using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Mvc;

namespace UWPSyncSampleWebServer.Controllers
{
    [Route("api/[controller]")]
    public class ValuesController : Controller
    {

        // proxy to handle requests and send them to SqlSyncProvider
        private WebProxyServerProvider webProxyServer;

        // Injected thanks to Dependency Injection
        public ValuesController(WebProxyServerProvider proxy)
        {
            webProxyServer = proxy;
        }

        // POST api/values
        [HttpPost]
        public async Task Post()
        {
            await webProxyServer.HandleRequestAsync(this.HttpContext);
        }

    }
}
