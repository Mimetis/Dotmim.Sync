using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Xamarin.Essentials;
using Xamarin.Forms;
using XamSyncSample.Models;
using XamSyncSample.Services;

namespace XamSyncSample.Context
{
    public class ContosoContext : DbContext
    {
        public ContosoContext()
        {
        }

        public DbSet<Employee> Employee { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            // Use this when you want to use sqlite on ios
            SQLitePCL.Batteries_V2.Init();

            var settings = DependencyService.Get<ISettingServices>();
            optionsBuilder.UseSqlite(settings.DataSourcePath);
        }
    }


}

