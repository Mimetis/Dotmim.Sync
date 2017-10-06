using Dotmim.Sync.Web;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Dotmim.Sync.SampleWebserver.Controllers
{
    [Route("api/[controller]")]
    public class ValuesController : Controller
    {
        // GET api/values
        [HttpGet]
        public IEnumerable<string> Get()
        {
            return new string[] { "value1", "value2" };
        }
      
        // proxy to handle requests and send them to SqlSyncProvider
        private WebProxyServerProvider webProxyServer;

        // Injected thanks to Dependency Injection
        public ValuesController(WebProxyServerProvider proxy)
        {
            webProxyServer = proxy;
        }

        // Handle all requests :)
        [HttpPost]
        public async Task Post()
        {
            await webProxyServer.HandleRequestAsync(this.HttpContext);
        }
    }


}
