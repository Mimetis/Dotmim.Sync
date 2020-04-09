using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using Xunit;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class SetupFilterTests
    {

        [Fact]
        public void SetupFilter_Compare_TwoSetupFilters_ShouldBe_Equals()
        {
            var filter1 = new SetupFilter();
            var filter2 = new SetupFilter();

            Assert.Equal(filter1, filter2);
            Assert.True(filter1 == filter2);

            filter1 = new SetupFilter("Product");
            filter2 = new SetupFilter("Product");

            Assert.Equal(filter1, filter2);
            Assert.True(filter1 == filter2);

            filter1 = new SetupFilter("Product", "");
            filter2 = new SetupFilter("Product");

            Assert.Equal(filter1, filter2);
            Assert.True(filter1 == filter2);

            filter1 = new SetupFilter("Product");
            filter2 = new SetupFilter("Product", string.Empty);

            Assert.Equal(filter1, filter2);
            Assert.True(filter1 == filter2);

            filter1 = new SetupFilter("Product", "dbo");
            filter2 = new SetupFilter("Product", "dbo");

            Assert.Equal(filter1, filter2);
            Assert.True(filter1 == filter2);
        }

        [Fact]
        public void SetupFilter_Compare_TwoSetupFilters_ShouldBe_Different()
        {

            var filter1 = new SetupFilter("Product1");
            var filter2 = new SetupFilter("Product2");

            Assert.NotEqual(filter1, filter2);
            Assert.False(filter1 == filter2);

            filter1 = new SetupFilter("Product", "");
            filter2 = new SetupFilter("Product", "d");

            Assert.NotEqual(filter1, filter2);
            Assert.False(filter1 == filter2);

            filter1 = new SetupFilter("Product", "d");
            filter2 = new SetupFilter("Product", string.Empty);

            Assert.NotEqual(filter1, filter2);
            Assert.False(filter1 == filter2);

            filter1 = new SetupFilter("Product", "dbo");
            filter2 = new SetupFilter("Product", "SalesLT");

            Assert.NotEqual(filter1, filter2);
            Assert.False(filter1 == filter2);
        }

        [Fact]
        public void SetupFilter_Compare_TwoSetupFilters_With_Parameters_ShouldBe_Equals()
        {
            var filter1 = new SetupFilter();
            var filter2 = new SetupFilter();

            filter1.AddParameter("ProductId", "Product");
            filter2.AddParameter("ProductId", "Product");

            Assert.Equal(filter1, filter2);
            Assert.True(filter1 == filter2);

            filter1 = new SetupFilter();
            filter2 = new SetupFilter();

            filter1.AddParameter("ProductId", "Product", true);
            filter2.AddParameter("ProductId", "Product", true);

            Assert.Equal(filter1, filter2);
            Assert.True(filter1 == filter2);

            filter1 = new SetupFilter();
            filter2 = new SetupFilter();

            filter1.AddParameter("ProductId", "Product", true, "12");
            filter2.AddParameter("ProductId", "Product", true, "12");

            Assert.Equal(filter1, filter2);
            Assert.True(filter1 == filter2);

            filter1 = new SetupFilter();
            filter2 = new SetupFilter();

            filter1.AddParameter("ProductId", DbType.Int32);
            filter2.AddParameter("ProductId", DbType.Int32);

            Assert.Equal(filter1, filter2);
            Assert.True(filter1 == filter2);

            filter1 = new SetupFilter();
            filter2 = new SetupFilter();

            filter1.AddParameter("ProductId", DbType.Int32, true);
            filter2.AddParameter("ProductId", DbType.Int32, true);

            Assert.Equal(filter1, filter2);
            Assert.True(filter1 == filter2);

            filter1 = new SetupFilter();
            filter2 = new SetupFilter();

            filter1.AddParameter("ProductId", DbType.Int32, true, "12");
            filter2.AddParameter("ProductId", DbType.Int32, true, "12");

            Assert.Equal(filter1, filter2);
            Assert.True(filter1 == filter2);

        }

        [Fact]
        public void SetupFilter_Compare_TwoSetupFilters_With_Parameters_ShouldBe_Different()
        {
            var filter1 = new SetupFilter();
            var filter2 = new SetupFilter();

            filter1.AddParameter("Product", "Product");
            filter2.AddParameter("ProductId", "Product");

            Assert.NotEqual(filter1, filter2);
            Assert.False(filter1 == filter2);

            filter1 = new SetupFilter();
            filter2 = new SetupFilter();

            filter1.AddParameter("ProductId", "Product1");
            filter2.AddParameter("ProductId", "Product2");

            Assert.NotEqual(filter1, filter2);
            Assert.False(filter1 == filter2);

            filter1 = new SetupFilter();
            filter2 = new SetupFilter();

            filter1.AddParameter("ProductId", "Product", false);
            filter2.AddParameter("ProductId", "Product", true);

            Assert.NotEqual(filter1, filter2);
            Assert.False(filter1 == filter2);

            filter1 = new SetupFilter();
            filter2 = new SetupFilter();

            filter1.AddParameter("ProductId", "Product", true, "2");
            filter2.AddParameter("ProductId", "Product", true, "12");

            Assert.NotEqual(filter1, filter2);
            Assert.False(filter1 == filter2);

            filter1 = new SetupFilter();
            filter2 = new SetupFilter();

            filter1.AddParameter("ProductId", DbType.String);
            filter2.AddParameter("ProductId", DbType.Int32);

            Assert.NotEqual(filter1, filter2);
            Assert.False(filter1 == filter2);

            filter1 = new SetupFilter();
            filter2 = new SetupFilter();

            filter1.AddParameter("ProductId", DbType.Int32, true);
            filter2.AddParameter("ProductId", DbType.Int32, false);

            Assert.NotEqual(filter1, filter2);
            Assert.False(filter1 == filter2);

            filter1 = new SetupFilter();
            filter2 = new SetupFilter();

            filter1.AddParameter("ProductId", DbType.Int32, true, "2");
            filter2.AddParameter("ProductId", DbType.Int32, true, "12");

            Assert.NotEqual(filter1, filter2);
            Assert.False(filter1 == filter2);
        }

        [Fact]
        public void SetupFilter_Compare_TwoSetupFilters_With_CustomWhere_ShouldBe_Equals()
        {
            var filter1 = new SetupFilter();
            var filter2 = new SetupFilter();

            filter1.AddCustomerWhere("where 1");
            filter2.AddCustomerWhere("where 1");

            Assert.Equal(filter1, filter2);
            Assert.True(filter1 == filter2);

            filter1 = new SetupFilter();
            filter2 = new SetupFilter();

            filter1.AddCustomerWhere("");
            filter2.AddCustomerWhere("");

            Assert.Equal(filter1, filter2);
            Assert.True(filter1 == filter2);

            filter1.AddCustomerWhere(null);
            filter2.AddCustomerWhere(null);

            Assert.Equal(filter1, filter2);
            Assert.True(filter1 == filter2);

            filter1.AddCustomerWhere("");
            filter2.AddCustomerWhere(null);

            Assert.Equal(filter1, filter2);
            Assert.True(filter1 == filter2);

        }

        [Fact]
        public void SetupFilter_Compare_TwoSetupFilters_With_CustomWhere_ShouldBe_Different()
        {
            var filter1 = new SetupFilter();
            var filter2 = new SetupFilter();

            filter1.AddCustomerWhere("where 1");
            filter2.AddCustomerWhere("where 2");

            Assert.NotEqual(filter1, filter2);
            Assert.False(filter1 == filter2);

            filter1 = new SetupFilter();
            filter2 = new SetupFilter();

            filter1.AddCustomerWhere("");
            filter2.AddCustomerWhere("a");

            Assert.NotEqual(filter1, filter2);
            Assert.False(filter1 == filter2);

            filter1.AddCustomerWhere("b");
            filter2.AddCustomerWhere(null);

            Assert.NotEqual(filter1, filter2);
            Assert.False(filter1 == filter2);

        }

        [Fact]
        public void SetupFilter_Compare_TwoSetupFilters_With_Joins_ShouldBe_Equals()
        {
            var filter1 = new SetupFilter();
            var filter2 = new SetupFilter();

            filter1.AddJoin(Join.Inner, "Product").On("Product", "ProductId", "ProductCategory", "ProductId");
            filter2.AddJoin(Join.Inner, "Product").On("Product", "ProductId", "ProductCategory", "ProductId");

            Assert.Equal(filter1, filter2);
            Assert.True(filter1 == filter2);
        }

        [Fact]
        public void SetupFilter_Compare_TwoSetupFilters_With_Joins_ShouldBe_Different()
        {
            var filter1 = new SetupFilter();
            var filter2 = new SetupFilter();

            filter1.AddJoin(Join.Inner, "Product1").On("Product", "ProductId", "ProductCategory", "ProductId");
            filter2.AddJoin(Join.Inner, "Product2").On("Product", "ProductId", "ProductCategory", "ProductId");

            Assert.NotEqual(filter1, filter2);
            Assert.False(filter1 == filter2);

            filter1 = new SetupFilter();
            filter2 = new SetupFilter();

            filter1.AddJoin(Join.Inner, "Product").On("Product1", "ProductId", "ProductCategory", "ProductId");
            filter2.AddJoin(Join.Inner, "Product").On("Product2", "ProductId", "ProductCategory", "ProductId");

            Assert.NotEqual(filter1, filter2);
            Assert.False(filter1 == filter2);

            filter1.AddJoin(Join.Inner, "Product").On("Product", "ProductId1", "ProductCategory", "ProductId");
            filter2.AddJoin(Join.Inner, "Product").On("Product", "ProductId2", "ProductCategory", "ProductId");

            Assert.NotEqual(filter1, filter2);
            Assert.False(filter1 == filter2);

            filter1.AddJoin(Join.Inner, "Product").On("Product", "ProductId", "ProductCategory1", "ProductId");
            filter2.AddJoin(Join.Inner, "Product").On("Product", "ProductId", "ProductCategory2", "ProductId");

            Assert.NotEqual(filter1, filter2);
            Assert.False(filter1 == filter2);

            filter1.AddJoin(Join.Inner, "Product").On("Product", "ProductId", "ProductCategory", "ProductId1");
            filter2.AddJoin(Join.Inner, "Product").On("Product", "ProductId", "ProductCategory", "ProductId2");

            Assert.NotEqual(filter1, filter2);
            Assert.False(filter1 == filter2);

        }


        [Fact]
        public void SetupFilter_Compare_TwoSetupFilters_With_Where_ShouldBe_Equals()
        {
            var filter1 = new SetupFilter();
            var filter2 = new SetupFilter();

            filter1.AddParameter("ProductId", DbType.Guid);
            filter2.AddParameter("ProductId", DbType.Guid);
            filter1.AddWhere("ProductId", "Product", "ProductId", "dbo");
            filter2.AddWhere("ProductId", "Product", "ProductId", "dbo");

            Assert.Equal(filter1, filter2);
            Assert.True(filter1 == filter2);

            filter1 = new SetupFilter();
            filter2 = new SetupFilter();

            filter1.AddParameter("ProductId", DbType.Guid);
            filter2.AddParameter("ProductId", DbType.Guid);
            filter1.AddWhere("ProductId", "Product", "ProductId");
            filter2.AddWhere("ProductId", "Product", "ProductId");

            Assert.Equal(filter1, filter2);
            Assert.True(filter1 == filter2);

            filter1 = new SetupFilter();
            filter2 = new SetupFilter();

            filter1.AddParameter("ProductId", DbType.Guid);
            filter2.AddParameter("ProductId", DbType.Guid);
            filter1.AddWhere("ProductId", "Product", "ProductId", "");
            filter2.AddWhere("ProductId", "Product", "ProductId");

            Assert.Equal(filter1, filter2);
            Assert.True(filter1 == filter2);

            filter1 = new SetupFilter();
            filter2 = new SetupFilter();

            filter1.AddParameter("ProductId", DbType.Guid);
            filter2.AddParameter("ProductId", DbType.Guid);
            filter1.AddWhere("ProductId", "Product", "ProductId");
            filter2.AddWhere("ProductId", "Product", "ProductId", "");

            Assert.Equal(filter1, filter2);
            Assert.True(filter1 == filter2);

        }

        [Fact]
        public void SetupFilter_Compare_TwoSetupFilters_With_Where_ShouldBe_Different()
        {
            var filter1 = new SetupFilter();
            var filter2 = new SetupFilter();


            filter1.AddParameter("ProductId", DbType.Guid);
            filter2.AddParameter("ProductId", DbType.Guid);
            filter1.AddWhere("ProductId", "Product", "ProductId", "dbo");
            filter2.AddWhere("ProductId", "Product", "ProductId", "SalesLT");

            Assert.NotEqual(filter1, filter2);
            Assert.False(filter1 == filter2);

            filter1 = new SetupFilter();
            filter2 = new SetupFilter();

            filter1.AddParameter("ProductId", DbType.Guid);
            filter2.AddParameter("ProductId", DbType.Guid);
            filter1.AddWhere("ProductId1", "Product", "ProductId");
            filter2.AddWhere("ProductId2", "Product", "ProductId");

            Assert.NotEqual(filter1, filter2);
            Assert.False(filter1 == filter2);

            filter1 = new SetupFilter();
            filter2 = new SetupFilter();

            filter1.AddParameter("ProductId", DbType.Guid);
            filter2.AddParameter("ProductId", DbType.Guid);
            filter1.AddWhere("ProductId", "Product1", "ProductId", "");
            filter2.AddWhere("ProductId", "Product2", "ProductId");

            Assert.NotEqual(filter1, filter2);
            Assert.False(filter1 == filter2);

            filter1 = new SetupFilter();
            filter2 = new SetupFilter();

            filter1.AddParameter("ProductId1", DbType.Guid);
            filter2.AddParameter("ProductId2", DbType.Guid);
            filter1.AddWhere("ProductId", "Product", "ProductId1");
            filter2.AddWhere("ProductId", "Product", "ProductId2");

            Assert.NotEqual(filter1, filter2);
            Assert.False(filter1 == filter2);

        }
    }
}
