using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Text;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class SyncTableTests : IDisposable
    {
        // Current test running
        private ITest test;
        private Stopwatch stopwatch;
        public ITestOutputHelper Output { get; }

        public SyncTableTests(ITestOutputHelper output)
        {

            // Getting the test running
            this.Output = output;
            var type = output.GetType();
            var testMember = type.GetField("test", BindingFlags.Instance | BindingFlags.NonPublic);
            this.test = (ITest)testMember.GetValue(output);
            this.stopwatch = Stopwatch.StartNew();
        }

        public void Dispose()
        {
            this.stopwatch.Stop();

            var str = $"{test.TestCase.DisplayName} : {this.stopwatch.Elapsed.Minutes}:{this.stopwatch.Elapsed.Seconds}.{this.stopwatch.Elapsed.Milliseconds}";
            Console.WriteLine(str);
            Debug.WriteLine(str);
        }


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
