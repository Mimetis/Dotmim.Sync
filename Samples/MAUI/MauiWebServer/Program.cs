using Dotmim.Sync;
using Dotmim.Sync.SqlServer;

namespace MauiWebServer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = WebApplication.CreateBuilder(args);

            // Add services to the container.
            // [Required]: Handling multiple sessions
            builder.Services.AddDistributedMemoryCache();
            builder.Services.AddSession(options => options.IdleTimeout = TimeSpan.FromMinutes(30));

            // [Required]: Get a connection string to your server data source
            var connectionString = builder.Configuration.GetSection("ConnectionStrings")["SqlConnection"];

            //var tables = new string[] { "Culinary.RecipeMethodContent" };

            var setup = new SyncSetup("Codes");

            // add a SqlSyncProvider acting as the server hub
            var provider = new SqlSyncProvider(connectionString);
            builder.Services.AddSyncServer(provider, setup);

            builder.Services.AddControllers();

            var app = builder.Build();

            // Configure the HTTP request pipeline.

            app.UseHttpsRedirection();

            app.UseAuthorization();

            app.UseSession();

            app.MapControllers();

            app.Run();
        }
    }
}