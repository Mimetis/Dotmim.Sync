using System.Net.Http;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using Dotmim.Sync.Web.Server;

namespace UWPSyncSampleWebServer.Controllers
{
    [RoutePrefix("api/values")]
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
        [Route("")]
        public async Task<HttpResponseMessage> Post()
        {
            return await webProxyServer.HandleRequestAsync(this.Request, new HttpContextWrapper(HttpContext.Current));
        }

        [HttpGet]
        [Route("")]
        public string Get()
        {
            return "hello world";
        }

    }
}
