using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using XamSyncSample.Context;
using XamSyncSample.Models;

namespace XamSyncSample.Services
{
    public class ContosoServices : IContosoServices
    {

        private Employee GetEmployeeFromReader(DbDataReader reader)
        {

            Employee emp = new Employee();
            emp.EmployeeId = reader[0] != DBNull.Value ? reader.GetGuid(0) : Guid.NewGuid();
            emp.FirstName = reader[1] != DBNull.Value ? reader.GetString(1) : null;
            emp.LastName = reader[2] != DBNull.Value ? reader.GetString(2) : null;
            emp.ProfilePicture = reader[3] != DBNull.Value ? reader.GetValue(3) as byte[] : null;
            emp.PhoneNumber = reader[4] != DBNull.Value ? reader.GetString(4) : null;
            emp.HireDate = reader[5] != DBNull.Value ? reader.GetDateTime(5) : DateTime.Now;
            emp.Comments = reader[6] != DBNull.Value ? reader.GetString(6) : null;
            return emp;

        }

        public void DeleteEmployee(Guid employeeId)
        {
            using (var dbContext = new ContosoContext())
            {
                var empQuery = from emp in dbContext.Employee
                               where emp.EmployeeId == employeeId
                               select emp;

                var employee = empQuery.FirstOrDefault();

                if (employee == null)
                    return;

                dbContext.Employee.Remove(employee);

                dbContext.SaveChanges();
            }
        }

        public Employee GetEmployee(Guid employeeId)
        {

            using (var dbContext = new ContosoContext())
            {
                var empQuery = from emp in dbContext.Employee
                               where emp.EmployeeId == employeeId
                               select emp;

                return empQuery.FirstOrDefault();
            }
        }

        public IEnumerable<Employee> GetEmployees()
        {
            using (var dbContext = new ContosoContext())
            {
                return dbContext.Employee.OrderBy(e => e.FirstName).ToList();
            }
        }

        public void SaveEmployee(Employee employee)
        {
            using (var dbContext = new ContosoContext())
            {
                var empQuery = from emp in dbContext.Employee
                               where emp.EmployeeId == employee.EmployeeId
                               select emp;

                var exist = empQuery.Any();

                if (!exist)
                {
                    dbContext.Employee.Add(employee);
                }
                else
                {

                    dbContext.Employee.Attach(employee);
                    dbContext.Entry(employee).State = EntityState.Modified;
                }

                dbContext.SaveChanges();
            }
        }
    }
}
