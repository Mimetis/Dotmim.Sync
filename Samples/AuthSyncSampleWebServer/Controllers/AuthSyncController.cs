using Dotmim.Sync.Web;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Threading.Tasks;

namespace AuthSyncSampleWebServer.Controllers
{
[Authorize]
[Route("api/[controller]")]
public class AuthSyncController : Controller
{
    // proxy to handle requests and send them to SqlSyncProvider
    private WebProxyServerProvider webProxyServer;

    // Injected thanks to Dependency Injection
    public AuthSyncController(WebProxyServerProvider proxy)
    {
        webProxyServer = proxy;
    }

    // POST api/values
    [HttpPost]
    public async Task Post()
    {
        // Checking the scope is optional
        // The [Authorize] class attribute is enough, since it prevents anyone to access
        // this controller without a Bearer token
        // Anyway you can have a more detailed control using the claims !
        string scope = (User.FindFirst("http://schemas.microsoft.com/identity/claims/scope"))?.Value;
        string user = (User.FindFirst(ClaimTypes.NameIdentifier))?.Value;

        if (scope != "access_as_user")
        {
            this.HttpContext.Response.StatusCode = StatusCodes.Status401Unauthorized;
            return;
        }

        await webProxyServer.HandleRequestAsync(this.HttpContext);
    }

}
}
