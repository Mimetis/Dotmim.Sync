using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Web.Server
{
    public class WebServerProperties
    {
        public const string Key = "WebServerProperties";

        public WebServerProperties() { }
       
        public Type ProviderType { get; set; }
        public WebServerOptions Options { get; set; }
        public string ConnectionString { get; set; }
        public SyncSetup Setup { get; set; }

        /// <summary>
        /// Copy properties from remote web orchestrator 
        /// </summary>
        public void SetValues(WebServerOrchestrator webServerOrchestrator)
        {
            this.Options = webServerOrchestrator.Options;
            this.Setup = webServerOrchestrator.Setup;
            this.ConnectionString = webServerOrchestrator.Provider.ConnectionString;
            this.ProviderType = webServerOrchestrator.Provider.GetType();
        }

        /// <summary>
        /// Set values
        /// </summary>
        public void SetValues(Type providerType, WebServerOptions options, string connectionString, SyncSetup setup)
        {
            this.ProviderType = providerType;
            this.Options = options;
            this.ConnectionString = connectionString;
            this.Setup = setup;
        }

        /// <summary>
        /// Create a new web server orchestrator
        /// </summary>
        public WebServerOrchestrator CreateWebServerOrchestrator()
        {
            // Create provicer
            var provider = (CoreProvider)Activator.CreateInstance(this.ProviderType);
            provider.ConnectionString = this.ConnectionString;

            // Create orchestrator
            var webServerOrchestrator = new WebServerOrchestrator(provider, this.Options, this.Setup);

            return webServerOrchestrator;
        }


    }
}
