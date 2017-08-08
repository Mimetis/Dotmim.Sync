using Dotmim.Sync.Proxy;
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

        // GET api/values/5
        [HttpGet("{id}")]
        public string Get(int id)
        {

            var webHost = Program.Host;
            var wpsp = webHost.Services.GetService<WebProxyServerProvider>();

            return TempData["TheInstance"] as string;

        }

        // POST api/values
        [HttpPost]
        public async Task Post()
        {
            await webProxyServer.HandleRequestAsync(this.HttpContext);

        }

        private WebProxyServerProvider webProxyServer;

        public ValuesController(WebProxyServerProvider proxy)
        {
            webProxyServer = proxy;
        }



        // PUT api/values/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE api/values/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }
    }


}
