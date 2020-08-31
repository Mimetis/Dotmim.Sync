using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Web.Server
{
    /// <summary>
    /// Define all properties for a server scope sync
    /// </summary>
    public class WebServerProperty
    {
        public Type ProviderType { get; set; }
        public WebServerOptions Options { get; set; }
        public string ConnectionString { get; set; }
        public SyncSetup Setup { get; set; }

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

    }
}
