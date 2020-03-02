//using System;
//using System.Threading.Tasks;
//using System.IO;
//using System.Linq;
//using System.Threading;
//using Newtonsoft.Json;
//using Dotmim.Sync.Serialization;
//using Dotmim.Sync.Web.Client;
//using Microsoft.AspNetCore.Http;
//using Microsoft.Extensions.Caching.Memory;
//using Microsoft.Extensions.DependencyInjection;
//using System.IO.Compression;
//using System.Text;
//using Microsoft.AspNetCore.Hosting;
//using System.Collections.Generic;

//namespace Dotmim.Sync.Web.Server
//{
//    /// <summary>
//    /// Class used when you have to deal with a Web Server
//    /// </summary>
//    public class WebProxyServerOrchestrator
//    {
//        public WebServerProperties WebServerProperties { get; }

//        /// <summary>
//        /// Default constructor for DI
//        /// </summary>
//        public WebProxyServerOrchestrator()
//        {
//        }

//        /// <summary>
//        /// Gets or Sets a Web server orchestratot that will override the one from cache
//        /// </summary>
//        public WebServerOrchestrator WebServerOrchestrator { get; set; }

//    }
//}
