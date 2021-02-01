using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using XamSyncSample.Models;

namespace XamSyncSample.Services
{
    public interface IContosoServices
    {
        void SaveEmployee(Employee employee);
        void DeleteEmployee(Guid employeeId);
        Employee GetEmployee(Guid employeeId);
        IEnumerable<Employee> GetEmployees();
    }
}
