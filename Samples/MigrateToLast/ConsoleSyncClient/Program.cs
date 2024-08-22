using McMaster.Extensions.CommandLineUtils;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;

namespace WebSyncClient
{
    internal class Program
    {

        // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql
        private static void Main(string[] args)
        {

            var configuration = new ConfigurationBuilder()
              .AddJsonFile("appsettings.json", false, true)
              .AddJsonFile("appsettings.local.json", true, true)
              .Build();

            var syncUri = configuration.GetSection("Api")["SyncAddress"];
            var sqliteConnection = configuration.GetConnectionString("SqliteConnection");

            var services = new ServiceCollection();
            services.AddSingleton<IConfiguration>(configuration);
            services.Configure<ApiOptions>(options => configuration.Bind("Api", options));

            var servicesProvider = services.BuildServiceProvider();

            var app = new CommandLineApplication<Board>();

            app.Conventions.UseDefaultConventions()
                           .UseConstructorInjection(servicesProvider);

            app.ShowHelp();

            do
            {
                // Console.Clear();
                Console.WriteLine("Enter you command line arguments:");
                Console.WriteLine();
                try
                {
                    var debugArgs = Console.ReadLine();
                    if (debugArgs != null)
                    {
                        app = new CommandLineApplication<Board>();
                        app.Conventions.UseDefaultConventions().UseConstructorInjection(servicesProvider);

                        var debugArgsArray = debugArgs.Split(" ");
                        app.Execute(debugArgsArray);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }

                Console.WriteLine();
                Console.WriteLine("Hit 'Esc' to end or another key to restart");
            }
            while (true);
        }
    }
}