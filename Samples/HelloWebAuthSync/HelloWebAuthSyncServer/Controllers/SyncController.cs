using Dotmim.Sync;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using System.Threading.Tasks;

namespace HelloWebSyncServer.Controllers
{
    [Authorize]
    [ApiController]
    [Route("api/[controller]")]
    public class SyncController : ControllerBase
    {
        private WebServerAgent webServerAgent;

        // Injected thanks to Dependency Injection
        public SyncController(WebServerAgent webServerAgent) => this.webServerAgent = webServerAgent;

        /// <summary>
        /// This POST handler is mandatory to handle all the sync process.
        [HttpPost]
        public async Task Post()
        {
            // If you are using the [Authorize] attribute you don't need to check
            // the User.Identity.IsAuthenticated value
            if (this.HttpContext.User.Identity.IsAuthenticated)
            {
                // OPTIONAL: -------------------------------------------
                // OPTIONAL: Playing with user coming from bearer token
                // OPTIONAL: -------------------------------------------

                // on each request coming from the client, just inject the User Id parameter
                this.webServerAgent.OnHttpGettingRequest(args =>
                {
                    // var pUserId = args.Context.Parameters["UserId"];

                    // if (pUserId == null)
                    // {
                    //    var userId = this.HttpContext.User.Claims.FirstOrDefault(x => x.Type == ClaimTypes.NameIdentifier);
                    //    args.Context.Parameters.Add("UserId", userId.Value);
                    // }
                    if (!args.Context.Parameters.Contains("IsActive"))
                        args.Context.Parameters.Add(new SyncParameter("IsActive", null));
                    else
                        args.Context.Parameters["IsActive"].Value = null;
                });

                // Because we don't want to send back this value, remove it from the response
                this.webServerAgent.OnHttpSendingResponse(args =>
                {
                    // if (args.Context.AdditionalProperties.ContainsKey("UserId"))
                    //    args.Context.AdditionalProperties.Remove("UserId");
                });

                await this.webServerAgent.HandleRequestAsync(this.HttpContext).ConfigureAwait(false);
            }
            else
            {
                this.HttpContext.Response.StatusCode = StatusCodes.Status401Unauthorized;
            }
        }

        /// <summary>
        /// This GET handler is optional. It allows you to see the configuration hosted on the server
        /// The configuration is shown only if Environmenent == Development.
        /// </summary>
        [HttpGet]
        [AllowAnonymous]
        public Task Get() => this.HttpContext.WriteHelloAsync(this.webServerAgent);
    }
}