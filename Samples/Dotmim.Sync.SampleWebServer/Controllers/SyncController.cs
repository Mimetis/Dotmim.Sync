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
        private WebProxyServerOrchestrator webProxyServer;

        // Injected thanks to Dependency Injection
        public SyncController(WebProxyServerOrchestrator proxy) => this.webProxyServer = proxy;

        [HttpPost]
        public async Task Post()
        {
            webProxyServer.WebServerOrchestrator.OnApplyChangesFailed(e =>
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

            

            await webProxyServer.HandleRequestAsync(this.HttpContext);
        }
    }
}
