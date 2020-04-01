using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class SyncTableTests
    {
        [Fact]
        public void SyncTable_Equals_ShouldWork()
        {
            SyncTable table1 = new SyncTable("Customer");
            SyncTable table2 = new SyncTable("Customer");

            Assert.Equal(table1, table2);
            Assert.True(table1.Equals(table2));

            SyncTable table3 = new SyncTable("ProductCategory", "SalesLT");
            SyncTable table4 = new SyncTable("ProductCategory", "SalesLT");

            Assert.Equal(table3, table4);
            Assert.True(table3.Equals(table4));
        }

        [Fact]
        public void SyncTable_EnsureSchema_Check_ColumnsAndRows()
        {
            SyncTable tCustomer = new SyncTable("Customer");
            tCustomer.Columns.Add(new SyncColumn("ID", typeof(Guid)));
            tCustomer.Columns.Add(new SyncColumn("Name", typeof(string)));

            SyncRow tCustomerRow = new SyncRow(tCustomer);
            tCustomerRow["ID"] = "A";
            tCustomerRow["Name"] = "B";

            tCustomer.Rows.Add(tCustomerRow);

            tCustomer.EnsureTable(new SyncSet());

            Assert.Equal(tCustomer, tCustomer.Columns.Table);
            Assert.Equal(tCustomer, tCustomer.Rows.Table);

        }
    }
}
