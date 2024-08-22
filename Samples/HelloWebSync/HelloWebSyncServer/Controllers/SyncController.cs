using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace HelloWebSyncServer.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class SyncController : ControllerBase
    {
        private WebServerAgent webServerAgent;
        private readonly IWebHostEnvironment env;

        // Injected thanks to Dependency Injection
        public SyncController(WebServerAgent webServerAgent, IWebHostEnvironment env)
        {
            this.webServerAgent = webServerAgent;
            this.env = env;
        }

        /// <summary>
        /// This POST handler is mandatory to handle all the sync process.
        /// </summary>
        /// <returns><placeholder>A <see cref="Task"/> representing the asynchronous operation.</placeholder></returns>
        [HttpPost]
        public Task Post()
            => this.webServerAgent.HandleRequestAsync(this.HttpContext);

        /// <summary>
        /// This GET handler is optional. It allows you to see the configuration hosted on the server.
        /// </summary>
        [HttpGet]
        public async Task Get()
        {
            if (this.env.IsDevelopment())
            {
                await this.HttpContext.WriteHelloAsync(this.webServerAgent);
            }
            else
            {
                var stringBuilder = new StringBuilder();

                stringBuilder.AppendLine("<!doctype html>");
                stringBuilder.AppendLine("<html>");
                stringBuilder.AppendLine("<title>Web Server properties</title>");
                stringBuilder.AppendLine("<body>");
                stringBuilder.AppendLine(" PRODUCTION. Write Whatever You Want Here ");
                stringBuilder.AppendLine("</body>");
                await this.HttpContext.Response.WriteAsync(stringBuilder.ToString()).ConfigureAwait(false);
            }
        }
    }
}