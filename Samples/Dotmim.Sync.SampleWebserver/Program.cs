using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Dotmim.Sync.SampleWebserver
{
    public class Program
    {

        private static IWebHost webHost;
        public static IWebHost Host
        {
            get
            {
                return webHost;
            }
        }

        public static void Main(string[] args)
        {
            webHost = BuildWebHost(args);
            webHost.Run();

        }

        public static IWebHost BuildWebHost(string[] args) =>
            WebHost.CreateDefaultBuilder(args)
                .UseStartup<Startup>()
                .Build();
        

    }
}
