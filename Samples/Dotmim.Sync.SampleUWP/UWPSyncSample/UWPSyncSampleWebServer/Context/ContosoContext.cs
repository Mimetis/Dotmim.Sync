
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
        public string ConnectionString { get; private set; }
        public IConfiguration Configuration { get; private set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            var connectionString = Configuration.GetConnectionString("ContosoConnection");
            optionsBuilder.UseSqlServer(connectionString);
        }

        public Task EnsureDatabaseCreatedAsync() => Database.EnsureCreatedAsync();

    }



}
