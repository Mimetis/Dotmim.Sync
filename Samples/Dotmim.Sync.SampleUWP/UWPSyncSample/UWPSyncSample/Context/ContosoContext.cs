using Autofac;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UWPSyncSample.Helpers;

namespace UWPSyncSample.Context
{
    public class ContosoContext : DbContext
    {
        private ConnectionType contosoType;

        public ContosoContext()
        {
            this.contosoType = ConnectionType.Client_SqlServer;
        }

        public ContosoContext(ConnectionType contosoType)
        {
            this.contosoType = contosoType;

        }
        public DbSet<Employee> Employees { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            var settingsHelper = ContainerHelper.Current.Container.Resolve<SettingsHelper>();

            switch (this.contosoType)
            {
                case ConnectionType.Client_SqlServer:
                    optionsBuilder.UseSqlServer(settingsHelper[ConnectionType.Client_SqlServer]);
                    break;
                case ConnectionType.Client_Sqlite:
                    optionsBuilder.UseSqlite(settingsHelper[ConnectionType.Client_Sqlite]);
                    break;
                case ConnectionType.Client_MySql:
                    optionsBuilder.UseMySql(settingsHelper[ConnectionType.Client_MySql]);
                    break;
            }
        }


    }

 

}
