using Dotmim.Sync.Tests.Misc;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Dotmim.Sync.Tests
{
    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    public class TestToto
    {
        [Fact, TestPriority(1)]
        public void Test()
        {
            long result;
            bool b = long.TryParse("20180711081340067", out result);
            Assert.Equal(b, true);
            Console.WriteLine(result);
        }
    }
}