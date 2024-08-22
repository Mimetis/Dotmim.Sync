using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Mvc;

namespace MauiWebServer.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class SyncController : ControllerBase
    {
        private WebServerAgent webServerAgent;

        private readonly ILogger<SyncController> logger;

        public SyncController(WebServerAgent webServerAgent, ILogger<SyncController> logger)
        {
            this.webServerAgent = webServerAgent;
            this.logger = logger;
        }

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