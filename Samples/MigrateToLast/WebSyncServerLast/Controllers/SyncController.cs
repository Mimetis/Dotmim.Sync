using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace WebSyncServerLast.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class SyncController : ControllerBase
    {
        private IEnumerable<WebServerAgent> webServerAgents;

        // Injected thanks to Dependency Injection
        public SyncController(IEnumerable<WebServerAgent> webServerAgents)
            => this.webServerAgents = webServerAgents;

        /// <summary>
        /// This POST handler is mandatory to handle all the sync process.
        /// </summary>
        [HttpPost]
        public async Task Post()
        {
            var identifier = this.HttpContext.GetIdentifier();

            var webserverAgent = this.webServerAgents.FirstOrDefault(c => c.Identifier == identifier);

            await webserverAgent.HandleRequestAsync(this.HttpContext).ConfigureAwait(false);
        }

        /// <summary>
        /// This GET handler is optional. It allows you to see the configuration hosted on the server
        /// The configuration is shown only if Environmenent == Development.
        /// </summary>
        [HttpGet]
        public Task Get() => this.HttpContext.WriteHelloAsync(this.webServerAgents);
    }
}