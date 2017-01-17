using Dotmim.Sync.SampleConsole;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using System;
using System.IO;

class Program
{
    static void Main(string[] args)
    {
        LaunchKestrelServer(args);

        Console.WriteLine("Hello World!");
    }

    private static void LaunchKestrelServer(string[] args)
    {
        Console.WriteLine("Running demo with Kestrel.");

        var config = new ConfigurationBuilder()
            //.AddCommandLine(args)
            .Build();

        var builder = new WebHostBuilder()
            .UseContentRoot(Directory.GetCurrentDirectory())
            .UseConfiguration(config)
            .UseStartup<Startup>()
            .UseKestrel()
            .UseUrls("http://localhost:5000");

        var host = builder.Build();
        host.Run();
        
    }

}