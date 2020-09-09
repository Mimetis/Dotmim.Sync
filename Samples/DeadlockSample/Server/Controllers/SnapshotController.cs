using Dotmim.Sync;
using Dotmim.Sync.SqlServer;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using System.Threading.Tasks;

namespace Server.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class SnapshotController : ControllerBase
    {
        private readonly IConfiguration configuration;
        private readonly SyncOptions syncOptions;
        private readonly SyncSetup syncSetup;

        public SnapshotController(IConfiguration configuration, SyncOptions syncOptions, SyncSetup syncSetup)
        {
            this.configuration = configuration;
            this.syncOptions = syncOptions;
            this.syncSetup = syncSetup;
        }

        [HttpPost]
        public async Task<IActionResult> CreateSnapshotAsync()
        {
            var connectionString = configuration.GetConnectionString("DefaultConnection");
            var serverProvider = new SqlSyncProvider(connectionString);
            // Create a remote orchestrator
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, syncOptions, syncSetup);
            // Create a snapshot
            await remoteOrchestrator.CreateSnapshotAsync();
            return Ok();
        }
    }
}
