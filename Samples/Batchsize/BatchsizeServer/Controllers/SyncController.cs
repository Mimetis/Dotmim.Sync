using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using System.Threading.Tasks;

namespace BatchsizeServer.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class SyncController : ControllerBase
    {
        private WebServerAgent webServerAgent;

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncController"/> class.
        /// Injecting the web server agent, containing the RemoteOrchestrator, thanks to Dependency Injection.
        /// </summary>
        public SyncController(WebServerAgent webServerAgent) => this.webServerAgent = webServerAgent;

        /// <summary>
        /// This POST handler is mandatory to handle all the sync process.
        /// </summary>
        [HttpPost]
        public Task Post() => this.webServerAgent.HandleRequestAsync(this.HttpContext);

        /// <summary>
        /// This GET handler is optional. It allows you to see the configuration hosted on the server
        /// The configuration is shown only if Environmenent == Development.
        /// </summary>
        [HttpGet]
        public Task Get() => this.HttpContext.WriteHelloAsync(this.webServerAgent);
    }
}