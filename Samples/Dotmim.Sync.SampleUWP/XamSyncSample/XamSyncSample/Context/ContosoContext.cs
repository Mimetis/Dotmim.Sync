using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Diagnostics;
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
    public class ContosoContext : DbContext, IDataStore<Employee>
    {
        public ContosoContext()
        {
        }

        public DbSet<Employee> Employees { get; set; }

        public async Task<bool> AddEmployeeAsync(Employee item)
        {
            this.Employees.Add(item);
            var entries = await this.SaveChangesAsync();
            return entries > 0;
        }
        public async Task<bool> DeleteEmployeeAsync(Guid id)
        {
            var employee = this.Employees.Find(id);

            if (employee != null)
            {
                this.Employees.Remove(employee);
                var entries = await this.SaveChangesAsync();
                return entries > 0;
            }

            return false;

        }
        public Task<Employee> GetEmployeeAsync(Guid id)
        {
            return this.Employees.FirstOrDefaultAsync(e => e.EmployeeId == id);
        }
        public async Task<List<Employee>> GetEmployeesAsync(bool forceRefresh = false)
        {
            try
            {

                var employees = await this.Employees.AsNoTracking().ToListAsync();

                return employees;
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
                throw;
            }

        }
        public async Task<bool> UpdateEmployeeAsync(Employee item)
        {
            var employee = this.Employees.Find(item.EmployeeId);

            if (employee == null)
            {
                employee = new Employee();
                this.Employees.Add(employee);
            }

            employee.FirstName = item.FirstName;
            employee.LastName = item.LastName;
            employee.PhoneNumber = item.PhoneNumber;
            employee.ProfilePicture = item.ProfilePicture;
            employee.ProfilePictureFileName = item.ProfilePictureFileName;
            employee.Comments = item.Comments;
            employee.HireDate = item.HireDate;

            var entries = await this.SaveChangesAsync();
            return entries > 0;
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            // Use this when you want to use sqlite on ios
            SQLitePCL.Batteries_V2.Init();

            var settings = DependencyService.Get<ISettingServices>();
            optionsBuilder.UseSqlite(settings.DataSource);
        }
    }


}

