using Dotmim.Sync;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Text;
using System.Threading.Tasks;
using UWPSyncSampleWebServer.Context;
using Microsoft.Extensions.Caching.Memory;
using Dotmim.Sync.Web.Client;

namespace UWPSyncSampleWebServer.Controllers
{
    [Route("api/[controller]")]
    public class SyncController : Controller
    {

        private WebServerManager manager;
        private readonly ContosoContext context;

        // Injected thanks to Dependency Injection
        public SyncController(WebServerManager manager, ContosoContext context)
        {
            this.manager = manager;
            this.context = context;
        }

        // POST api/values
        [HttpPost]
        public async Task Post()
        {

       

            await manager.HandleRequestAsync(this.HttpContext);
        }
        [HttpGet]
        public async Task Get()
        {
            await context.EnsureDatabaseCreatedAsync();

            await manager.HandleRequestAsync(this.HttpContext);
        }

        [HttpGet("snapshot/{scopeName}")]
        public async Task Snapshot(string scopeName)
        {
            var orchestrator = this.manager.GetOrchestrator(scopeName);

            var stringBuilder = new StringBuilder();

            var progress = new Progress<ProgressArgs>(args =>
            {
                stringBuilder.AppendLine($"<div>{args.Message}</div>");
            });


            stringBuilder.AppendLine("<!doctype html>");
            stringBuilder.AppendLine("<html>");
            stringBuilder.AppendLine("<head>");
            stringBuilder.AppendLine("</head>");
            stringBuilder.AppendLine("<title>Web Server properties</title>");
            stringBuilder.AppendLine("<body>");
            stringBuilder.AppendLine("<h2>Generating Snapshot</h2>");

            var snap = await orchestrator.CreateSnapshotAsync(progress: progress);

            stringBuilder.AppendLine("</div>");
            stringBuilder.AppendLine("</body>");
            stringBuilder.AppendLine("</html>");


            await this.HttpContext.Response.WriteAsync(stringBuilder.ToString());

        }

    }
}
