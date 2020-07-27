
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UWPSyncSampleWebServer.Context
{
    public class ContosoContext : DbContext
    {

        public ContosoContext()
        {
        }


        public ContosoContext(IConfiguration configuration)
        {
            this.Configuration = configuration;
        }

        public DbSet<Employee> Employees { get; set; }
        public string ConnectionString { get; }
        public IConfiguration Configuration { get; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            // Get a connection string for your server data source
            var connectionString = Configuration.GetSection("ConnectionStrings")["DefaultConnection"];

            optionsBuilder.UseSqlServer(this.ConnectionString);

        }

        public Task EnsureDatabaseCreatedAsync() => Database.EnsureCreatedAsync();

    }



}
