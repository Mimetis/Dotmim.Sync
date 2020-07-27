using System;
using System.Collections.Generic;

namespace Dotmim.Sync.Tests.Models
{
    public partial class Address
    {
        public Address()
        {
            CustomerAddress = new HashSet<CustomerAddress>();
            EmployeeAddress = new HashSet<EmployeeAddress>();
            SalesOrderHeaderBillToAddress = new HashSet<SalesOrderHeader>();
            SalesOrderHeaderShipToAddress = new HashSet<SalesOrderHeader>();
        }

        public int AddressId { get; set; }
        public string AddressLine1 { get; set; }
        public string AddressLine2 { get; set; }
        public string City { get; set; }
        public string StateProvince { get; set; }
        public string CountryRegion { get; set; }
        public string PostalCode { get; set; }
        public Guid? Rowguid { get; set; }
        public DateTime? ModifiedDate { get; set; }

        public ICollection<CustomerAddress> CustomerAddress { get; set; }
        public ICollection<EmployeeAddress> EmployeeAddress { get; set; }
        public ICollection<SalesOrderHeader> SalesOrderHeaderBillToAddress { get; set; }
        public ICollection<SalesOrderHeader> SalesOrderHeaderShipToAddress { get; set; }
    }
}
