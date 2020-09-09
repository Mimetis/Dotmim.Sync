using System.Threading.Tasks;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Mvc;

namespace MoneyWallet.Backend.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class SyncController : ControllerBase
    {
        // The WebServerManager instance is useful to manage all
        // the Web server orchestrators registered in the Startup.cs
        private readonly WebServerManager manager;

        // Injected thanks to Dependency Injection
        public SyncController(WebServerManager manager) => this.manager = manager;

        /// <summary>
        /// This POST handler is mandatory to handle all the sync process
        /// </summary>
        [HttpPost]
        public async Task PostAsync() =>
            await manager.HandleRequestAsync(this.HttpContext);

        /// <summary>
        /// This GET handler is optional. It allows you to see the configuration hosted on the server
        /// The configuration is shown only if Environment == Development
        /// </summary>
        [HttpGet]
        public async Task GetAsync() =>
            await manager.HandleRequestAsync(this.HttpContext);
    }
}
