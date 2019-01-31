using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Mvc;

namespace Dotmim.Sync.SampleWebServer.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class SyncController : ControllerBase
    {
        private WebProxyServerProvider webProxyServer;

        // Injected thanks to Dependency Injection
        public SyncController(WebProxyServerProvider proxy)
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
