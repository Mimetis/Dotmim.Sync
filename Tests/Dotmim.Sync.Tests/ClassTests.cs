using Dotmim.Sync.Test.SqlUtils;
using Dotmim.Sync.Tests.Misc;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Tests
{
    public class ClassFixtureTests : IDisposable
    {
        public ClassFixtureTests()
        {
            Debug.WriteLine("Constructor Fixture called");
        }
        public void Dispose()
        {
            Debug.WriteLine("Dispose Fixture called");
        }
    }

    public class ClassTests : IClassFixture<ClassFixtureTests>
    {
        ClassFixtureTests fixture;

        public ClassTests(ClassFixtureTests fixture)
        {
            this.fixture = fixture;

            Debug.WriteLine("Constructor Class Test called");
        }

        [Theory]
        [ClassData(typeof(InlineConfigurations))]
        public async Task Initialize(SyncConfiguration conf)
        {
            var c = conf;
            Debug.WriteLine("Initialize method called");
            Assert.True(true);
            await Task.CompletedTask;
        }

        [Fact]
        public async Task Sync()
        {
            Debug.WriteLine("Sync method called");
            Assert.True(true);
            await Task.CompletedTask;
        }
    }
}
