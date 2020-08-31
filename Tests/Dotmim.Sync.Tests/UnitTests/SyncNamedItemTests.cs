using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class SyncNamedItemTests : IDisposable
    {
        // Current test running
        private ITest test;
        private Stopwatch stopwatch;
        public ITestOutputHelper Output { get; }

        public SyncNamedItemTests(ITestOutputHelper output)
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
        public void Compare_Two_Enumerable_Empty_ShouldBe_Count_Equals()
        {
            var lst1 = new List<string>();
            var lst2 = new List<string>();

            Assert.True(lst1.CompareWith(lst2));
        }


        [Fact]
        public void Compare_Two_Enumerable_ShouldBe_Count_Equals()
        {
            var lst1 = new List<string>() { "1" };
            var lst2 = new List<string>() { "1" };

            Assert.True(lst1.CompareWith(lst2));
        }

        [Fact]
        public void Compare_Two_Enumerable_FirstIsNull_ShouldNotBe_Count_Equals()
        {
            List<string> lst1 = null;
            var lst2 = new List<string>() { "2" };

            Assert.False(lst1.CompareWith(lst2));
        }

        [Fact]
        public void Compare_Two_Enumerable_SecondIsNull_ShouldNotBe_Count_Equals()
        {
            var lst1 = new List<string>() { "2" };
            List<string> lst2 = null;

            Assert.False(lst1.CompareWith(lst2));
        }

        [Fact]
        public void Compare_Two_Enumerable_Null_ShouldBe_Count_Equals()
        {
            List<string> lst1 = null;
            List<string> lst2 = null;

            Assert.True(lst1.CompareWith(lst2));
        }


        [Fact]
        public void Compare_Two_Enumerable_Different_ShouldNotBe_Equals()
        {
            var lst1 = new List<string>() { "1" };
            var lst2 = new List<string>() { "2" };

            Assert.False(lst1.CompareWith(lst2));
        }


        [Fact]
        public void Compare_SyncColumnIdentifier_When_Empty_ShouldBe_Equals()
        {
            var columnId1 = new SyncColumnIdentifier();
            var columnId2 = new SyncColumnIdentifier();

            var isNamedEquals = columnId1.EqualsByName(columnId2);

            // Check operator EqualsByName
            Assert.True(isNamedEquals);

            // Check operator == (who should make an equality on the references)
            Assert.False(columnId1 == columnId2);

            // Check default Equals which should use the EqualsByName as well
            Assert.Equal(columnId1, columnId2);
        }

        [Fact]
        public void Compare_SyncColumnIdentifier_When_OneField_NotEmpty_ShouldBe_Equals()
        {
            var columnId1 = new SyncColumnIdentifier("CustomerId", string.Empty);
            var columnId2 = new SyncColumnIdentifier("CustomerId", string.Empty);

            var isNamedEquals = columnId1.EqualsByName(columnId2);

            Assert.True(isNamedEquals);
            Assert.False(columnId1 == columnId2);
            Assert.Equal(columnId1, columnId2);

        }

        [Fact]
        public void Compare_SyncColumnIdentifier_When_OneField_Empty_And_OtherField_Null_ShouldBe_Equals()
        {
            var columnId1 = new SyncColumnIdentifier("CustomerId", string.Empty);
            var columnId2 = new SyncColumnIdentifier("CustomerId", null, string.Empty);

            var isNamedEquals = columnId1.EqualsByName(columnId2);

            Assert.True(isNamedEquals);
            Assert.False(columnId1 == columnId2);
            Assert.Equal(columnId1, columnId2);

        }

        [Fact]
        public void Compare_SyncColumnIdentifier_When_OtherInstance_Null_ShouldNotBe_Equals()
        {
            SyncColumnIdentifier columnId1 = new SyncColumnIdentifier("CustomerId", string.Empty);
            SyncColumnIdentifier columnId2 = null;

            var isNamedEquals = columnId1.EqualsByName(columnId2);

            Assert.False(isNamedEquals);
            Assert.False(columnId1 == columnId2);
            Assert.NotEqual(columnId1, columnId2);

        }

        [Fact]
        public void Compare_SyncColumnIdentifier_When_Instances_Null_ShouldNotBe_Equals()
        {
            SyncColumnIdentifier columnId1 = null;
            SyncColumnIdentifier columnId2 = null;

            Assert.Throws<NullReferenceException>(() => columnId1.EqualsByName(columnId2));

        }


        [Fact]
        public void Compare_SyncColumnIdentifier_When_Property_IsDifferent_ShouldNotBe_Equals()
        {
            var columnId1 = new SyncColumnIdentifier("CustomerId1", "Customer");
            var columnId2 = new SyncColumnIdentifier("CustomerId2", "Customer");

            var isNamedEquals = columnId1.EqualsByName(columnId2);

            Assert.False(isNamedEquals);
        }

    }
}
