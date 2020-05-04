using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class SyncSetupTests
    {
        [Fact]
        public void SyncSetup_Compare_TwoSetup_ShouldBe_Equals()
        {
            SyncSetup setup1 = new SyncSetup(new string[] { "Product", "ProductCategory", "Employee" });
            SyncSetup setup2 = new SyncSetup(new string[] { "Product", "ProductCategory", "Employee" });

            Assert.Equal(setup1, setup2);
            Assert.True(setup1.Equals(setup2));

            setup1 = new SyncSetup(new string[] { "Employee", "ProductCategory", "Product" });
            setup2 = new SyncSetup(new string[] { "Product", "ProductCategory", "Employee" });

            Assert.Equal(setup1, setup2);
            Assert.True(setup1.Equals(setup2));

            setup1 = new SyncSetup();
            setup2 = new SyncSetup();
            setup1.Tables.Add(new SetupTable("Employee", "dbo"));
            setup2.Tables.Add(new SetupTable("Employee", "dbo"));

            Assert.Equal(setup1, setup2);
            Assert.True(setup1.Equals(setup2));

            setup1 = new SyncSetup();
            setup2 = new SyncSetup();
            setup1.Tables.Add(new SetupTable("Employee", "dbo"));
            setup1.Tables.Add(new SetupTable("Product"));
            setup2.Tables.Add(new SetupTable("Employee", "dbo"));
            setup2.Tables.Add(new SetupTable("Product"));

            Assert.Equal(setup1, setup2);
            Assert.True(setup1.Equals(setup2));

            setup1 = new SyncSetup();
            setup2 = new SyncSetup();
            setup1.Tables.Add(new SetupTable("Product"));
            setup1.Tables.Add(new SetupTable("Employee", "dbo"));
            setup2.Tables.Add(new SetupTable("Employee", "dbo"));
            setup2.Tables.Add(new SetupTable("Product"));

            Assert.Equal(setup1, setup2);
            Assert.True(setup1.Equals(setup2));
        }

        [Fact]
        public void SyncSetup_Compare_TwoSetup_ShouldBe_Different()
        {
            SyncSetup setup1 = new SyncSetup(new string[] { "Product1", "ProductCategory", "Employee" });
            SyncSetup setup2 = new SyncSetup(new string[] { "Product2", "ProductCategory", "Employee" });

            Assert.NotEqual(setup1, setup2);
            Assert.False(setup1.Equals(setup2));

            setup1 = new SyncSetup(new string[] { "Employee", "ProductCategory1", "Product" });
            setup2 = new SyncSetup(new string[] { "Product", "ProductCategory2", "Employee" });

            Assert.NotEqual(setup1, setup2);
            Assert.False(setup1.Equals(setup2));

            setup1 = new SyncSetup();
            setup2 = new SyncSetup();
            setup1.Tables.Add(new SetupTable("Employee", ""));
            setup2.Tables.Add(new SetupTable("Employee", "dbo"));

            Assert.NotEqual(setup1, setup2);
            Assert.False(setup1.Equals(setup2));

            setup1 = new SyncSetup();
            setup2 = new SyncSetup();
            setup1.Tables.Add(new SetupTable("Employee", ""));
            setup1.Tables.Add(new SetupTable("Product"));
            setup2.Tables.Add(new SetupTable("Employee", "dbo"));
            setup2.Tables.Add(new SetupTable("Product"));

            Assert.NotEqual(setup1, setup2);
            Assert.False(setup1.Equals(setup2));

            setup1 = new SyncSetup();
            setup2 = new SyncSetup();
            setup1.Tables.Add(new SetupTable("Product"));
            setup1.Tables.Add(new SetupTable("Employee", "dbo"));
            setup2.Tables.Add(new SetupTable("Employee", ""));
            setup2.Tables.Add(new SetupTable("Product"));

            Assert.NotEqual(setup1, setup2);
            Assert.False(setup1.Equals(setup2));
        }

        [Fact]
        public void SyncSetup_Compare_TwoSetup_Properties_ShouldBe_Equals()
        {
            var setup1 = new SyncSetup(new string[] { "Employee", "ProductCategory", "Product" });
            var setup2 = new SyncSetup(new string[] { "Product", "ProductCategory", "Employee" });
            setup1.StoredProceduresPrefix = "sp";
            setup2.StoredProceduresPrefix = "sp";

            Assert.Equal(setup1, setup2);
            Assert.True(setup1.Equals(setup2));

            setup1 = new SyncSetup(new string[] { "Employee", "ProductCategory", "Product" });
            setup2 = new SyncSetup(new string[] { "Product", "ProductCategory", "Employee" });
            setup1.StoredProceduresSuffix = "sp";
            setup2.StoredProceduresSuffix = "sp";

            Assert.Equal(setup1, setup2);
            Assert.True(setup1.Equals(setup2));

            setup1 = new SyncSetup(new string[] { "Employee", "ProductCategory", "Product" });
            setup2 = new SyncSetup(new string[] { "Product", "ProductCategory", "Employee" });
            setup1.TriggersPrefix = "sp";
            setup2.TriggersPrefix = "sp";

            Assert.Equal(setup1, setup2);
            Assert.True(setup1.Equals(setup2));

            setup1 = new SyncSetup(new string[] { "Employee", "ProductCategory", "Product" });
            setup2 = new SyncSetup(new string[] { "Product", "ProductCategory", "Employee" });
            setup1.TriggersSuffix = "sp";
            setup2.TriggersSuffix = "sp";

            Assert.Equal(setup1, setup2);
            Assert.True(setup1.Equals(setup2));

            setup1 = new SyncSetup(new string[] { "Employee", "ProductCategory", "Product" });
            setup2 = new SyncSetup(new string[] { "Product", "ProductCategory", "Employee" });
            setup1.TrackingTablesPrefix = "sp";
            setup2.TrackingTablesPrefix = "sp";

            Assert.Equal(setup1, setup2);
            Assert.True(setup1.Equals(setup2));

            setup1 = new SyncSetup(new string[] { "Employee", "ProductCategory", "Product" });
            setup2 = new SyncSetup(new string[] { "Product", "ProductCategory", "Employee" });
            setup1.TrackingTablesSuffix = "sp";
            setup2.TrackingTablesSuffix = "sp";

            Assert.Equal(setup1, setup2);
            Assert.True(setup1.Equals(setup2));

            setup1 = new SyncSetup(new string[] { "Employee", "ProductCategory", "Product" });
            setup2 = new SyncSetup(new string[] { "Product", "ProductCategory", "Employee" });
            setup1.Version = "1";
            setup2.Version = "1";

            Assert.Equal(setup1, setup2);
            Assert.True(setup1.Equals(setup2));
        }
        [Fact]
        public void SyncSetup_Compare_TwoSetup_Properties_ShouldBe_Different()
        {
            var setup1 = new SyncSetup(new string[] { "Employee", "ProductCategory", "Product" });
            var setup2 = new SyncSetup(new string[] { "Product", "ProductCategory", "Employee" });
            setup1.StoredProceduresPrefix = "sp1";
            setup2.StoredProceduresPrefix = "sp";

            Assert.NotEqual(setup1, setup2);
            Assert.False(setup1.Equals(setup2));

            setup1 = new SyncSetup(new string[] { "Employee", "ProductCategory", "Product" });
            setup2 = new SyncSetup(new string[] { "Product", "ProductCategory", "Employee" });
            setup1.StoredProceduresPrefix = null;
            setup2.StoredProceduresPrefix = "sp";

            Assert.NotEqual(setup1, setup2);
            Assert.False(setup1.Equals(setup2));

            setup1 = new SyncSetup(new string[] { "Employee", "ProductCategory", "Product" });
            setup2 = new SyncSetup(new string[] { "Product", "ProductCategory", "Employee" });
            setup1.StoredProceduresSuffix = "sp1";
            setup2.StoredProceduresSuffix = "sp";

            Assert.NotEqual(setup1, setup2);
            Assert.False(setup1.Equals(setup2));

            setup1 = new SyncSetup(new string[] { "Employee", "ProductCategory", "Product" });
            setup2 = new SyncSetup(new string[] { "Product", "ProductCategory", "Employee" });
            setup1.StoredProceduresSuffix = null;
            setup2.StoredProceduresSuffix = "sp";

            Assert.NotEqual(setup1, setup2);
            Assert.False(setup1.Equals(setup2));

            setup1 = new SyncSetup(new string[] { "Employee", "ProductCategory", "Product" });
            setup2 = new SyncSetup(new string[] { "Product", "ProductCategory", "Employee" });
            setup1.TriggersPrefix = "sp1";
            setup2.TriggersPrefix = "sp";

            Assert.NotEqual(setup1, setup2);
            Assert.False(setup1.Equals(setup2));

            setup1 = new SyncSetup(new string[] { "Employee", "ProductCategory", "Product" });
            setup2 = new SyncSetup(new string[] { "Product", "ProductCategory", "Employee" });
            setup1.TriggersPrefix = "sp1";
            setup2.TriggersPrefix = null;

            Assert.NotEqual(setup1, setup2);
            Assert.False(setup1.Equals(setup2));

            setup1 = new SyncSetup(new string[] { "Employee", "ProductCategory", "Product" });
            setup2 = new SyncSetup(new string[] { "Product", "ProductCategory", "Employee" });
            setup1.TriggersSuffix = "sp1";
            setup2.TriggersSuffix = "sp";

            Assert.NotEqual(setup1, setup2);
            Assert.False(setup1.Equals(setup2));

            setup1 = new SyncSetup(new string[] { "Employee", "ProductCategory", "Product" });
            setup2 = new SyncSetup(new string[] { "Product", "ProductCategory", "Employee" });
            setup1.TriggersSuffix = "sp1";
            setup2.TriggersSuffix = null;

            Assert.NotEqual(setup1, setup2);
            Assert.False(setup1.Equals(setup2));

            setup1 = new SyncSetup(new string[] { "Employee", "ProductCategory", "Product" });
            setup2 = new SyncSetup(new string[] { "Product", "ProductCategory", "Employee" });
            setup1.TrackingTablesPrefix = "sp1";
            setup2.TrackingTablesPrefix = "sp";

            Assert.NotEqual(setup1, setup2);
            Assert.False(setup1.Equals(setup2));

            setup1 = new SyncSetup(new string[] { "Employee", "ProductCategory", "Product" });
            setup2 = new SyncSetup(new string[] { "Product", "ProductCategory", "Employee" });
            setup1.TrackingTablesPrefix = null;
            setup2.TrackingTablesPrefix = "sp";

            Assert.NotEqual(setup1, setup2);
            Assert.False(setup1.Equals(setup2));

            setup1 = new SyncSetup(new string[] { "Employee", "ProductCategory", "Product" });
            setup2 = new SyncSetup(new string[] { "Product", "ProductCategory", "Employee" });
            setup1.TrackingTablesSuffix = "sp1";
            setup2.TrackingTablesSuffix = "sp";

            Assert.NotEqual(setup1, setup2);
            Assert.False(setup1.Equals(setup2));

            setup1 = new SyncSetup(new string[] { "Employee", "ProductCategory", "Product" });
            setup2 = new SyncSetup(new string[] { "Product", "ProductCategory", "Employee" });
            setup1.TrackingTablesSuffix = null;
            setup2.TrackingTablesSuffix = "sp";

            Assert.NotEqual(setup1, setup2);
            Assert.False(setup1.Equals(setup2));

            setup1 = new SyncSetup(new string[] { "Employee", "ProductCategory", "Product" });
            setup2 = new SyncSetup(new string[] { "Product", "ProductCategory", "Employee" });
            setup1.Version = "D";
            setup2.Version = "1";

            setup1 = new SyncSetup(new string[] { "Employee", "ProductCategory", "Product" });
            setup2 = new SyncSetup(new string[] { "Product", "ProductCategory", "Employee" });
            setup1.Version = "D";
            setup2.Version = null;

            Assert.NotEqual(setup1, setup2);
            Assert.False(setup1.Equals(setup2));
        }

        [Fact]
        public void SyncSetup_Compare_TwoSetup_With_Filters_ShouldBe_Equals()
        {
            SyncSetup setup1 = new SyncSetup(new string[] { "Customer", "Product", "ProductCategory", "Employee" });
            SyncSetup setup2 = new SyncSetup(new string[] { "Customer", "Product", "ProductCategory", "Employee" });

            setup1.Filters.Add("Customer", "CompanyName");

            var addressCustomerFilter = new SetupFilter("CustomerAddress");
            addressCustomerFilter.AddParameter("CompanyName", "Customer");
            addressCustomerFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            addressCustomerFilter.AddWhere("CompanyName", "Customer", "CompanyName");
            setup1.Filters.Add(addressCustomerFilter);

            var addressFilter = new SetupFilter("Address");
            addressFilter.AddParameter("CompanyName", "Customer");
            addressFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "AddressId", "Address", "AddressId");
            addressFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            addressFilter.AddWhere("CompanyName", "Customer", "CompanyName");
            setup1.Filters.Add(addressFilter);

            var orderHeaderFilter = new SetupFilter("SalesOrderHeader");
            orderHeaderFilter.AddParameter("CompanyName", "Customer");
            orderHeaderFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
            orderHeaderFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            orderHeaderFilter.AddWhere("CompanyName", "Customer", "CompanyName");
            setup1.Filters.Add(orderHeaderFilter);

            var orderDetailsFilter = new SetupFilter("SalesOrderDetail");
            orderDetailsFilter.AddParameter("CompanyName", "Customer");
            orderDetailsFilter.AddJoin(Join.Left, "SalesOrderHeader").On("SalesOrderDetail", "SalesOrderID", "SalesOrderHeader", "SalesOrderID");
            orderDetailsFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
            orderDetailsFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            orderDetailsFilter.AddWhere("CompanyName", "Customer", "CompanyName");
            setup1.Filters.Add(orderDetailsFilter);

            setup2.Filters.Add("Customer", "CompanyName");

            var addressCustomerFilter2 = new SetupFilter("CustomerAddress");
            addressCustomerFilter2.AddParameter("CompanyName", "Customer");
            addressCustomerFilter2.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            addressCustomerFilter2.AddWhere("CompanyName", "Customer", "CompanyName");
            setup2.Filters.Add(addressCustomerFilter2);

            var addressFilter2 = new SetupFilter("Address");
            addressFilter2.AddParameter("CompanyName", "Customer");
            addressFilter2.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "AddressId", "Address", "AddressId");
            addressFilter2.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            addressFilter2.AddWhere("CompanyName", "Customer", "CompanyName");
            setup2.Filters.Add(addressFilter2);

            var orderHeaderFilter2 = new SetupFilter("SalesOrderHeader");
            orderHeaderFilter2.AddParameter("CompanyName", "Customer");
            orderHeaderFilter2.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
            orderHeaderFilter2.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            orderHeaderFilter2.AddWhere("CompanyName", "Customer", "CompanyName");
            setup2.Filters.Add(orderHeaderFilter2);

            var orderDetailsFilter2 = new SetupFilter("SalesOrderDetail");
            orderDetailsFilter2.AddParameter("CompanyName", "Customer");
            orderDetailsFilter2.AddJoin(Join.Left, "SalesOrderHeader").On("SalesOrderDetail", "SalesOrderID", "SalesOrderHeader", "SalesOrderID");
            orderDetailsFilter2.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
            orderDetailsFilter2.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            orderDetailsFilter2.AddWhere("CompanyName", "Customer", "CompanyName");
            setup2.Filters.Add(orderDetailsFilter2);


            Assert.Equal(setup1, setup2);
            Assert.True(setup1.Equals(setup2));
        }

        [Fact]
        public void SyncSetup_Compare_TwoSetup_With_Filters_ShouldBe_Different()
        {
            SyncSetup setup1 = new SyncSetup(new string[] { "Customer", "Address", "ProductCategory", "Employee" });
            SyncSetup setup2 = new SyncSetup(new string[] { "Customer", "Address", "ProductCategory", "Employee" });

            setup1.Filters.Add("Customer1", "CompanyName");

            var addressFilter = new SetupFilter("Address");
            addressFilter.AddParameter("CompanyName", "Customer");
            addressFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "AddressId", "Address", "AddressId");
            addressFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            addressFilter.AddWhere("CompanyName", "Customer", "CompanyName");
            setup1.Filters.Add(addressFilter);

            setup2.Filters.Add("Customer2", "CompanyName");

            var addressFilter2 = new SetupFilter("Address");
            addressFilter2.AddParameter("CompanyName", "Customer");
            addressFilter2.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "AddressId", "Address", "AddressId");
            addressFilter2.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            addressFilter2.AddWhere("CompanyName", "Customer", "CompanyName");
            setup2.Filters.Add(addressFilter2);

            Assert.NotEqual(setup1, setup2);
            Assert.False(setup1.Equals(setup2));

            setup1 = new SyncSetup(new string[] { "Customer", "Address", "ProductCategory", "Employee" });
            setup2 = new SyncSetup(new string[] { "Customer", "Address", "ProductCategory", "Employee" });

            setup1.Filters.Add("Customer", "CompanyName");

            addressFilter = new SetupFilter("Address1");
            addressFilter.AddParameter("CompanyName", "Customer");
            addressFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "AddressId", "Address", "AddressId");
            addressFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            addressFilter.AddWhere("CompanyName", "Customer", "CompanyName");
            setup1.Filters.Add(addressFilter);

            setup2.Filters.Add("Customer", "CompanyName");

            addressFilter2 = new SetupFilter("Address2");
            addressFilter2.AddParameter("CompanyName", "Customer");
            addressFilter2.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "AddressId", "Address", "AddressId");
            addressFilter2.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            addressFilter2.AddWhere("CompanyName", "Customer", "CompanyName");
            setup2.Filters.Add(addressFilter2);

            Assert.NotEqual(setup1, setup2);
            Assert.False(setup1.Equals(setup2));

            setup1 = new SyncSetup(new string[] { "Customer", "Address", "ProductCategory", "Employee" });
            setup2 = new SyncSetup(new string[] { "Customer", "Address", "ProductCategory", "Employee" });

            setup1.Filters.Add("Customer", "CompanyName");

            addressFilter = new SetupFilter("Address");
            addressFilter.AddParameter("CompanyName1", "Customer");
            addressFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "AddressId", "Address", "AddressId");
            addressFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            addressFilter.AddWhere("CompanyName", "Customer", "CompanyName1");
            setup1.Filters.Add(addressFilter);

            setup2.Filters.Add("Customer", "CompanyName");

            addressFilter2 = new SetupFilter("Address");
            addressFilter2.AddParameter("CompanyName2", "Customer");
            addressFilter2.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "AddressId", "Address", "AddressId");
            addressFilter2.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            addressFilter2.AddWhere("CompanyName", "Customer", "CompanyName2");
            setup2.Filters.Add(addressFilter2);

            Assert.NotEqual(setup1, setup2);
            Assert.False(setup1.Equals(setup2));

            setup1 = new SyncSetup(new string[] { "Customer", "Address", "ProductCategory", "Employee" });
            setup2 = new SyncSetup(new string[] { "Customer", "Address", "ProductCategory", "Employee" });

            setup1.Filters.Add("Customer", "CompanyName");

            addressFilter = new SetupFilter("Address");
            addressFilter.AddParameter("CompanyName", "Customer");
            addressFilter.AddJoin(Join.Left, "CustomerAddress1").On("CustomerAddress", "AddressId", "Address", "AddressId");
            addressFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            addressFilter.AddWhere("CompanyName", "Customer", "CompanyName");
            setup1.Filters.Add(addressFilter);

            setup2.Filters.Add("Customer", "CompanyName");

            addressFilter2 = new SetupFilter("Address");
            addressFilter2.AddParameter("CompanyName", "Customer");
            addressFilter2.AddJoin(Join.Left, "CustomerAddress2").On("CustomerAddress", "AddressId", "Address", "AddressId");
            addressFilter2.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            addressFilter2.AddWhere("CompanyName", "Customer", "CompanyName");
            setup2.Filters.Add(addressFilter2);

            Assert.NotEqual(setup1, setup2);
            Assert.False(setup1.Equals(setup2));

            setup1 = new SyncSetup(new string[] { "Customer", "Address", "ProductCategory", "Employee" });
            setup2 = new SyncSetup(new string[] { "Customer", "Address", "ProductCategory", "Employee" });

            setup1.Filters.Add("Customer", "CompanyName");

            addressFilter = new SetupFilter("Address");
            addressFilter.AddParameter("CompanyName", "Customer");
            addressFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "AddressId", "Address", "AddressId");
            addressFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            addressFilter.AddWhere("CompanyName1", "Customer", "CompanyName");
            setup1.Filters.Add(addressFilter);

            setup2.Filters.Add("Customer", "CompanyName");

            addressFilter2 = new SetupFilter("Address");
            addressFilter2.AddParameter("CompanyName", "Customer");
            addressFilter2.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "AddressId", "Address", "AddressId");
            addressFilter2.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            addressFilter2.AddWhere("CompanyName2", "Customer", "CompanyName");
            setup2.Filters.Add(addressFilter2);

            Assert.NotEqual(setup1, setup2);
            Assert.False(setup1.Equals(setup2));

        }
    }
}
