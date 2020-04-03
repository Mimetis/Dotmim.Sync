using Dotmim.Sync.Setup;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using Xunit;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class SetupFilterJoinTests
    {

        [Fact]
        public void SetupFilterJoin_Compare_TwoSetupFilterJoin_ShouldBe_Equals()
        {
            SetupFilterJoin filterJoin1 = new SetupFilterJoin(Join.Inner, null, null, null, null, null);
            SetupFilterJoin filterJoin2 = new SetupFilterJoin(Join.Inner, null, null, null, null, null);

            Assert.Equal(filterJoin1, filterJoin2);
            Assert.True(filterJoin1.Equals(filterJoin2));

            filterJoin1 = new SetupFilterJoin(Join.Inner, "t1", null, null, null, null);
            filterJoin2 = new SetupFilterJoin(Join.Inner, "t1", null, null, null, null);

            Assert.Equal(filterJoin1, filterJoin2);
            Assert.True(filterJoin1.Equals(filterJoin2));

            filterJoin1 = new SetupFilterJoin(Join.Inner, null, "t1", null, null, null);
            filterJoin2 = new SetupFilterJoin(Join.Inner, null, "t1", null, null, null);

            Assert.Equal(filterJoin1, filterJoin2);
            Assert.True(filterJoin1.Equals(filterJoin2));

            filterJoin1 = new SetupFilterJoin(Join.Inner, null, null, "t1", null, null);
            filterJoin2 = new SetupFilterJoin(Join.Inner, null, null, "t1", null, null);

            Assert.Equal(filterJoin1, filterJoin2);
            Assert.True(filterJoin1.Equals(filterJoin2));

            filterJoin1 = new SetupFilterJoin(Join.Inner, null, null, null, "t1", null);
            filterJoin2 = new SetupFilterJoin(Join.Inner, null, null, null, "t1", null);

            Assert.Equal(filterJoin1, filterJoin2);
            Assert.True(filterJoin1.Equals(filterJoin2));

            filterJoin1 = new SetupFilterJoin(Join.Inner, null, null, null, null, "t1");
            filterJoin2 = new SetupFilterJoin(Join.Inner, null, null, null, null, "t1");

            Assert.Equal(filterJoin1, filterJoin2);
            Assert.True(filterJoin1.Equals(filterJoin2));
        }

        [Fact]
        public void SetupFilterJoin_Compare_TwoSetupFilterJoin_ShouldBe_Different()
        {
            SetupFilterJoin filterJoin1 = new SetupFilterJoin(Join.Inner, null, null, null, null, null);
            SetupFilterJoin filterJoin2 = new SetupFilterJoin(Join.Left, null, null, null, null, null);

            Assert.NotEqual(filterJoin1, filterJoin2);
            Assert.False(filterJoin1.Equals(filterJoin2));

            filterJoin1 = new SetupFilterJoin(Join.Inner, null, null, null, null, null);
            filterJoin2 = new SetupFilterJoin(Join.Inner, "t1", null, null, null, null);

            Assert.NotEqual(filterJoin1, filterJoin2);
            Assert.False(filterJoin1.Equals(filterJoin2));

            filterJoin1 = new SetupFilterJoin(Join.Inner, "t1", null, null, null, null);
            filterJoin2 = new SetupFilterJoin(Join.Inner, "t2", null, null, null, null);

            Assert.NotEqual(filterJoin1, filterJoin2);
            Assert.False(filterJoin1.Equals(filterJoin2));

            filterJoin1 = new SetupFilterJoin(Join.Inner, null, "t1", null, null, null);
            filterJoin2 = new SetupFilterJoin(Join.Inner, null, "t2", null, null, null);

            Assert.NotEqual(filterJoin1, filterJoin2);
            Assert.False(filterJoin1.Equals(filterJoin2));

            filterJoin1 = new SetupFilterJoin(Join.Inner, null, null, null, null, null);
            filterJoin2 = new SetupFilterJoin(Join.Inner, null, "t2", null, null, null);

            Assert.NotEqual(filterJoin1, filterJoin2);
            Assert.False(filterJoin1.Equals(filterJoin2));

            filterJoin1 = new SetupFilterJoin(Join.Inner, null, null, "t1", null, null);
            filterJoin2 = new SetupFilterJoin(Join.Inner, null, null, "t2", null, null);

            Assert.NotEqual(filterJoin1, filterJoin2);
            Assert.False(filterJoin1.Equals(filterJoin2));
            filterJoin1 = new SetupFilterJoin(Join.Inner, null, null, "t1", null, null);
            filterJoin2 = new SetupFilterJoin(Join.Inner, null, null, null, null, null);

            Assert.NotEqual(filterJoin1, filterJoin2);
            Assert.False(filterJoin1.Equals(filterJoin2));

            filterJoin1 = new SetupFilterJoin(Join.Inner, null, null, null, "t1", null);
            filterJoin2 = new SetupFilterJoin(Join.Inner, null, null, null, "t2", null);

            Assert.NotEqual(filterJoin1, filterJoin2);
            Assert.False(filterJoin1.Equals(filterJoin2));

            filterJoin1 = new SetupFilterJoin(Join.Inner, null, null, null, "t1", null);
            filterJoin2 = new SetupFilterJoin(Join.Inner, null, null, null, null, null);

            Assert.NotEqual(filterJoin1, filterJoin2);
            Assert.False(filterJoin1.Equals(filterJoin2));

            filterJoin1 = new SetupFilterJoin(Join.Inner, null, null, null, null, "t1");
            filterJoin2 = new SetupFilterJoin(Join.Inner, null, null, null, null, "t2");

            Assert.NotEqual(filterJoin1, filterJoin2);
            Assert.False(filterJoin1.Equals(filterJoin2));

            filterJoin1 = new SetupFilterJoin(Join.Inner, null, null, null, null, "t1");
            filterJoin2 = new SetupFilterJoin(Join.Inner, null, null, null, null, null);

            Assert.NotEqual(filterJoin1, filterJoin2);
            Assert.False(filterJoin1.Equals(filterJoin2));
        }

    }
}
