using Autofac;
using Microsoft.EntityFrameworkCore;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UWPSyncSample.Context;
using UWPSyncSample.Helpers;

namespace UWPSyncSample.Services
{
    public class ContosoServices : IContosoServices
    {
        private readonly ConnectionType contosoType;

        public ContosoServices(ConnectionType contosoType)
        {
            this.contosoType = contosoType;
        }

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
            //if (this.contosoType == ConnectionType.Client_MySql || this.contosoType == ConnectionType.Client_Http_MySql)
            //{
            //    var settingsHelper = ContainerHelper.Current.Container.Resolve<SettingsHelper>();
            //    var csString = settingsHelper[contosoType];

            //    try
            //    {
            //        using (var connection = new MySqlConnection(csString))
            //        {
            //            connection.Open();
            //            string commandtext = "DELETE FROM employees where employeeid = @employeeid";
            //            MySqlCommand command = new MySqlCommand(commandtext, connection);
            //            command.Parameters.AddWithValue("@employeeId", employeeId);

            //            command.ExecuteNonQuery();
            //            connection.Close();
            //        }
            //    }
            //    catch (Exception)
            //    {

            //        throw;
            //    }
            //    return;
            //}


            using (var dbContext = new ContosoContext(this.contosoType))
            {
                var empQuery = from emp in dbContext.Employees
                               where emp.EmployeeId == employeeId
                               select emp;

                var employee = empQuery.FirstOrDefault();

                if (employee == null)
                    return;

                dbContext.Employees.Remove(employee);

                dbContext.SaveChanges();
            }
        }

        public Employee GetEmployee(Guid employeeId)
        {

            //if (this.contosoType == ConnectionType.Client_MySql || this.contosoType == ConnectionType.Client_Http_MySql)
            //{
            //    var settingsHelper = ContainerHelper.Current.Container.Resolve<SettingsHelper>();
            //    var csString = settingsHelper[contosoType];

            //    try
            //    {
            //        using (var connection = new MySqlConnection(csString))
            //        {
            //            connection.Open();
            //            string commandtext = "SELECT * employees where employeeid = @employeeid";
            //            MySqlCommand command = new MySqlCommand(commandtext, connection);
            //            command.Parameters.AddWithValue("@employeeid", employeeId);
            //            Employee employee = null;

            //            using (var reader = command.ExecuteReader())
            //            {
            //                reader.Read();

            //                employee = GetEmployeeFromReader(reader);

            //            }
            //            connection.Close();
            //            return employee;
            //        }
            //    }
            //    catch (Exception)
            //    {

            //        throw;
            //    }
            //}

            using (var dbContext = new ContosoContext(this.contosoType))
            {
                var empQuery = from emp in dbContext.Employees
                               where emp.EmployeeId == employeeId
                               select emp;

                return empQuery.FirstOrDefault();
            }
        }

        public IEnumerable<Employee> GetEmployees()
        {

            //if (this.contosoType == ConnectionType.Client_MySql || this.contosoType == ConnectionType.Client_Http_MySql)
            //{
            //    var settingsHelper = ContainerHelper.Current.Container.Resolve<SettingsHelper>();
            //    var csString = settingsHelper[contosoType];

            //    try
            //    {
            //        using (var connection = new MySqlConnection(csString))
            //        {
            //            connection.Open();
            //            string commandtext = "SELECT * FROM employees";
            //            MySqlCommand command = new MySqlCommand(commandtext, connection);
            //            List<Employee> lstEmployees = null;
            //            using (var reader = command.ExecuteReader())
            //            {
            //                if (reader.HasRows)
            //                    lstEmployees = new List<Employee>();

            //                while (reader.Read())
            //                    lstEmployees.Add(GetEmployeeFromReader(reader));

            //            }
            //            connection.Close();
            //            return lstEmployees;
            //        }
            //    }
            //    catch (Exception)
            //    {

            //        throw;
            //    }
            //}

            using (var dbContext = new ContosoContext(this.contosoType))
            {
                return dbContext.Employees.OrderBy(e => e.FirstName).ToList();
            }
        }

        public void SaveEmployee(Employee employee)
        {
            using (var dbContext = new ContosoContext(this.contosoType))
            {
                var empQuery = from emp in dbContext.Employees
                               where emp.EmployeeId == employee.EmployeeId
                               select emp;

                var exist = empQuery.Any();

                if (!exist)
                {
                    dbContext.Employees.Add(employee);
                }
                else
                {

                    dbContext.Employees.Attach(employee);
                    dbContext.Entry(employee).State = EntityState.Modified;
                }

                dbContext.SaveChanges();
            }
        }
    }
}
