using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class SetupTableTests
    {


        [Fact]
        public void SetupTable_Compare_TwoSetupTables_ShouldBe_Equals()
        {
            SetupTable table1 = new SetupTable("Customer");
            SetupTable table2 = new SetupTable("Customer");

            Assert.Equal(table1, table2);
            Assert.True(table1.Equals(table2));

            SetupTable table3 = new SetupTable("ProductCategory", "SalesLT");
            SetupTable table4 = new SetupTable("ProductCategory", "SalesLT");

            Assert.Equal(table3, table4);
            Assert.True(table3.Equals(table4));
            Assert.False(table3 == table4);
        }

        [Fact]
        public void SetupTable_Compare_TwoSetupTables_ShouldBe_Different()
        {
            SetupTable table1 = new SetupTable("Customer1");
            SetupTable table2 = new SetupTable("Customer2");

            Assert.NotEqual(table1, table2);
            Assert.False(table1.Equals(table2));

            SetupTable table3 = new SetupTable("ProductCategory", "dbo");
            SetupTable table4 = new SetupTable("ProductCategory", "SalesLT");

            Assert.NotEqual(table3, table4);
            Assert.False(table3.Equals(table4));
        }

        [Fact]
        public void SetupTable_Compare_TwoSetupTables_WithSameSyncDirection_ShouldBe_Equals()
        {
            SetupTable table1 = new SetupTable("Customer");
            SetupTable table2 = new SetupTable("Customer");

            table1.SyncDirection = SyncDirection.Bidirectional;
            table2.SyncDirection = SyncDirection.Bidirectional;

            Assert.Equal(table1, table2);
            Assert.True(table1.Equals(table2));
        }

        [Fact]
        public void SetupTable_Compare_TwoSetupTables_WithDifferentSyncDirection_ShouldBe_Different()
        {
            SetupTable table1 = new SetupTable("Customer");
            SetupTable table2 = new SetupTable("Customer");

            table1.SyncDirection = SyncDirection.UploadOnly;
            table2.SyncDirection = SyncDirection.Bidirectional;

            Assert.NotEqual(table1, table2);
            Assert.False(table1.Equals(table2));
        }

        [Fact]
        public void SetupTable_Compare_TwoSetupTables_WithSameColumns_ShouldBe_Equals()
        {
            SetupTable table1 = new SetupTable("Customer");
            SetupTable table2 = new SetupTable("Customer");

            table1.Columns.Add("CustomerID");
            table2.Columns.Add("CustomerID");

            Assert.Equal(table1, table2);
            Assert.True(table1.Equals(table2));
        }
        [Fact]
        public void SetupTable_Compare_TwoSetupTables_WithDifferentColumns_ShouldBe_Equals()
        {
            SetupTable table1 = new SetupTable("Customer");
            SetupTable table2 = new SetupTable("Customer");

            table1.Columns.Add("CustomerID");

            Assert.NotEqual(table1, table2);
            Assert.False(table1.Equals(table2));

            table2.Columns.Add("ID");

            Assert.NotEqual(table1, table2);
            Assert.False(table1.Equals(table2));

        }
    }
}
