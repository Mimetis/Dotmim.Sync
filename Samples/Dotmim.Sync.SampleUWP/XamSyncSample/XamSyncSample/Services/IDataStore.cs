using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace XamSyncSample.Services
{
    public interface IDataStore<T>
    {
        Task<bool> AddEmployeeAsync(T item);
        Task<bool> UpdateEmployeeAsync(T item);
        Task<bool> DeleteEmployeeAsync(Guid id);
        Task<T> GetEmployeeAsync(Guid id);
        Task<List<T>> GetEmployeesAsync(bool forceRefresh = false);
    }
}
