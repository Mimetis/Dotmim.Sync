using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dotmim.Sync.Web.Server;
using Dotmim.Sync;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace HelloWebSyncServer.Controllers
{
[Authorize]
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
    [HttpPost]
    public async Task Post()
    {
        // If you are using the [Authorize] attribute you don't need to check
        // the User.Identity.IsAuthenticated value
        if (HttpContext.User.Identity.IsAuthenticated)
        {
            var orchestrator = webServerManager.GetOrchestrator(this.HttpContext);

            // on each request coming from the client, just inject the User Id parameter
            orchestrator.OnHttpGettingRequest(args =>
            {
                var pUserId = args.Context.Parameters["UserId"];

                if (pUserId == null)
                    args.Context.Parameters.Add("UserId", this.HttpContext.User.Identity.Name);

            });

            // Because we don't want to send back this value, remove it from the response 
            orchestrator.OnHttpSendingResponse(args =>
            {
                if (args.Context.Parameters.Contains("UserId"))
                    args.Context.Parameters.Remove("UserId");
            });

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
