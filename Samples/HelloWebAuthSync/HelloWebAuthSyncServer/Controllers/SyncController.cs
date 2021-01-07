using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace HelloWebSyncServer.Controllers
{
    //[Authorize]
    [ApiController]
    [Route("api/[controller]")]
    public class SyncController : ControllerBase
    {
        // The WebServerManager instance is useful to manage all the Web server orchestrators register in the Startup.cs
        private WebServerManager webServerManager;

        // Injected thanks to Dependency Injection
        public SyncController(WebServerManager webServerManager) => this.webServerManager = webServerManager;

        /// <summary>
        /// This POST handler is mandatory to handle all the sync process
        /// </summary>
        /// <returns></returns>
        [HttpPost]
        public async Task Post()
        {
            // If you are using the [Authorize] attribute you don't need to check
            // the User.Identity.IsAuthenticated value
            if (HttpContext.User.Identity.IsAuthenticated)
            {
                await webServerManager.HandleRequestAsync(this.HttpContext);
            }
            else
            {
                this.HttpContext.Response.StatusCode = StatusCodes.Status401Unauthorized;
            }
        }

        /// <summary>
        /// This GET handler is optional. It allows you to see the configuration hosted on the server
        /// The configuration is shown only if Environmenent == Development
        /// </summary>
        [HttpGet]
        [AllowAnonymous]
        public async Task Get() => await webServerManager.HandleRequestAsync(this.HttpContext);
    }
}
