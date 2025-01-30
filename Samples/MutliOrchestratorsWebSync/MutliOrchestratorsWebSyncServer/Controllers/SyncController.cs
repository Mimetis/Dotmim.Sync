using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MutliOrchestratorsWebSyncServer.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class SyncController : ControllerBase
    {
        private IEnumerable<WebServerAgent> webserverAgents;

        // Injected thanks to Dependency Injection
        public SyncController(IEnumerable<WebServerAgent> webServerAgents)
            => this.webserverAgents = webServerAgents;

        /// <summary>
        /// This POST handler is mandatory to handle all the sync process.
        /// </summary>
        [HttpPost]
        public async Task Post()
        {
            // get the scope name send by the client and the identifier
            var scopeName = this.HttpContext.GetScopeName();
            var identifier = this.HttpContext.GetIdentifier();

            // first retrieve all the web server agents configured for the correct server connection string:
            var agents = this.webserverAgents.Where(wsa => wsa.Identifier == identifier);

            if (agents == null || !agents.Any())
                throw new Exception("No web server agent found for the current identifier");

            // then on this list of agents, get the correct agent for the current scope name
            var webserverAgent = agents.FirstOrDefault(wsa => wsa.ScopeName == scopeName);

            if (webserverAgent == null)
                throw new Exception("no web server agent configured with this scope name");

            await webserverAgent.HandleRequestAsync(this.HttpContext).ConfigureAwait(false);
        }

        /// <summary>
        /// This GET handler is optional. It allows you to see the configuration hosted on the server
        /// The configuration is shown only if Environmenent == Development.
        /// </summary>
        [HttpGet]
        public Task Get() => this.HttpContext.WriteHelloAsync(this.webserverAgents);
    }
}