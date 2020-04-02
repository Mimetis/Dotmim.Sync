using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Mvc;

namespace Dotmim.Sync.SampleWebServer.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class SyncController : ControllerBase
    {
        private WebServerManager webServerManager;

        // Injected thanks to Dependency Injection
        public SyncController(WebServerManager webServerManager) => this.webServerManager = webServerManager;

        [HttpPost]
        public async Task Post()
        {
            // Get Orchestrator regarding the incoming scope name (from http context)
            var orchestrator = webServerManager.GetOrchestrator(this.HttpContext);

            orchestrator.OnApplyChangesFailed(e =>
            {
                if (e.Conflict.RemoteRow.Table.TableName == "Region")
                {
                    e.Resolution = ConflictResolution.MergeRow;
                    e.FinalRow["RegionDescription"] = "Eastern alone !";
                }
                else
                {
                    e.Resolution = ConflictResolution.ServerWins;
                }
            });

            var progress = new SynchronousProgress<ProgressArgs>(pa =>
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"{pa.Context.SyncStage}\t {pa.Message}");
                Console.ResetColor();
            });

            // handle request
            await webServerManager.HandleRequestAsync(this.HttpContext);
        }
    }
}
