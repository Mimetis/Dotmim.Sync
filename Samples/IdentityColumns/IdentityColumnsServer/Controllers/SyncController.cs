using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Caching.Memory;
using System.Data.Common;
using System.Data;
using Microsoft.Extensions.Configuration;
using Microsoft.Data.SqlClient;

namespace IdentityColumnsServer.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class SyncController : ControllerBase
    {
        // The WebServerManager instance is useful to manage all the Web server orchestrators register in the Startup.cs
        private WebServerManager webServerManager;
        private readonly ISeedingServices seedingServices;
        private readonly IConfiguration configuration;

        // Injected thanks to Dependency Injection
        public SyncController(WebServerManager webServerManager, ISeedingServices seedingServices, IConfiguration configuration)
        {
            this.webServerManager = webServerManager;
            this.seedingServices = seedingServices;
            this.configuration = configuration;
        }

        /// <summary>
        /// This POST handler is mandatory to handle all the sync process
        /// </summary>
        /// <returns></returns>
        [HttpPost]
        public async Task Post()
        {

            var orchestrator = webServerManager.GetOrchestrator(this.HttpContext);

            await orchestrator.HandleRequestAsync(HttpContext);
        }

        [HttpGet]
        [Route("seedings/{scopeId}")]
        public async Task<List<Seeding>> GetSeedingsAsync(Guid scopeId)
        {
            try
            {
                var connectionString = this.configuration.GetSection("ConnectionStrings")["SqlConnection"];

                var connection = new SqlConnection(connectionString);

                var seedings = await this.seedingServices.GetSeedingsAsync(scopeId, connection).ConfigureAwait(false);

                return seedings;

            }
            catch (Exception ex)
            {

                throw ex;
            }


        }



        /// <summary>
        /// This GET handler is optional. It allows you to see the configuration hosted on the server
        /// The configuration is shown only if Environmenent == Development
        /// </summary>
        [HttpGet]
        public async Task Get() => await webServerManager.HandleRequestAsync(this.HttpContext);

    }
}
