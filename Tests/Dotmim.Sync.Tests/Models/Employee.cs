using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Tests.Models
{
    public partial class Employee
    {
        public Employee()
        {
            EmployeeAddress = new HashSet<EmployeeAddress>();
            Customer = new HashSet<Customer>();
        }

        public int EmployeeId { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string RegionId { get; set; }
        public Guid? Rowguid { get; set; }
        public DateTime? ModifiedDate { get; set; }

        public Region Region { get; set; }
        public ICollection<Customer> Customer { get; set; }
        public ICollection<EmployeeAddress> EmployeeAddress { get; set; }
    }
}
