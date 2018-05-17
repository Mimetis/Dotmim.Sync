using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using Dotmim.Sync.Web;

namespace UWPSyncSampleWebServer.Controllers
{
    [Route("api/[controller]")]
    public class ValuesController : ApiController
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
            var context = HttpContext.Current;
            await webProxyServer.HandleRequestAsync(context);
        }

    }
}
