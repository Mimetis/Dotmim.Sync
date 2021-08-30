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
using System.Security.Claims;

namespace HelloWebSyncServer.Controllers
{
    [Authorize]
    [ApiController]
    [Route("api/[controller]")]
    public class SyncController : ControllerBase
    {
        public WebServerOrchestrator WebServerOrchestrator { get; }

        // Injected thanks to Dependency Injection
        public SyncController(WebServerOrchestrator webServerOrchestrator) => this.WebServerOrchestrator = webServerOrchestrator;

        /// <summary>
        /// This POST handler is mandatory to handle all the sync process
        [HttpPost]
        public async Task Post()
        {
            // If you are using the [Authorize] attribute you don't need to check
            // the User.Identity.IsAuthenticated value
            if (HttpContext.User.Identity.IsAuthenticated)
            {
                // on each request coming from the client, just inject the User Id parameter
                WebServerOrchestrator.OnHttpGettingRequest(args =>
                {
                    var pUserId = args.Context.Parameters["UserId"];

                    if (pUserId == null)
                    {
                        var userId = this.HttpContext.User.Claims.FirstOrDefault(x => x.Type == ClaimTypes.NameIdentifier);
                        args.Context.Parameters.Add("UserId", userId);
                    }

                });

                // Because we don't want to send back this value, remove it from the response 
                WebServerOrchestrator.OnHttpSendingResponse(args =>
                {
                    if (args.Context.Parameters.Contains("UserId"))
                        args.Context.Parameters.Remove("UserId");
                });

                await WebServerOrchestrator.HandleRequestAsync(this.HttpContext);
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
        public Task Get() => WebServerOrchestrator.WriteHelloAsync(this.HttpContext, WebServerOrchestrator);
    }
}
