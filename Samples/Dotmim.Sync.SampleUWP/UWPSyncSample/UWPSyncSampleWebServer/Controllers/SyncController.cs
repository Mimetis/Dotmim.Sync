using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Mvc;
using UWPSyncSampleWebServer.Context;

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
        // POST api/values
        [HttpGet]
        public async Task Get()
        {
            await context.EnsureDatabaseCreatedAsync();

            await manager.HandleRequestAsync(this.HttpContext);
        }

    }
}
