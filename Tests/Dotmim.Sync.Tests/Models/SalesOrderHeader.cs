using System;
using System.Collections.Generic;

namespace Dotmim.Sync.Tests.Models
{
    public partial class SalesOrderHeader
    {
        public SalesOrderHeader()
        {
            SalesOrderDetail = new HashSet<SalesOrderDetail>();
        }

        public int SalesOrderId { get; set; }
        public short RevisionNumber { get; set; }
        public DateTime? OrderDate { get; set; }
        public DateTime? DueDate { get; set; }
        public DateTimeOffset? ShipDate { get; set; }
        public short Status { get; set; }
        public bool? OnlineOrderFlag { get; set; }
        public string SalesOrderNumber { get; set; }
        public string PurchaseOrderNumber { get; set; }
        public string AccountNumber { get; set; }
        public Guid CustomerId { get; set; }
        public int? ShipToAddressId { get; set; }
        public int? BillToAddressId { get; set; }
        public string ShipMethod { get; set; }
        public string CreditCardApprovalCode { get; set; }
        public decimal SubTotal { get; set; }
        public decimal TaxAmt { get; set; }
        public decimal Freight { get; set; }

        public decimal TotalDue { get; set; }
        public string Comment { get; set; }
        public Guid? Rowguid { get; set; }
        public DateTime? ModifiedDate { get; set; }

        public Address BillToAddress { get; set; }
        public Customer Customer { get; set; }
        public Address ShipToAddress { get; set; }
        public ICollection<SalesOrderDetail> SalesOrderDetail { get; set; }
    }
}
