using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Test.Misc;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Microsoft.EntityFrameworkCore;
using System;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Tests
{

    /// <summary>
    /// This abstract class implements all the required tests.
    /// All the tests are agonistic to any provider 
    /// This class does not use XUnit annotations, since it can't be used as is.
    /// Each provider should inherits from this class and override methods, with XUnit annotations
    /// </summary>
    public abstract class FilterTestsBase
    {
        // Runner for my tests, that will run each tests on each client and on tcp then http
        protected readonly TestRunner testRunner;

        // abstract fixture used to run the tests
        protected readonly ProviderFixture fixture;

        protected virtual AdventureWorksContext GetServerDbContext() => new AdventureWorksContext(this.fixture);

        protected virtual AdventureWorksContext GetClientDbContext(ProviderRun providerRun) => new AdventureWorksContext(providerRun);

        protected virtual AdventureWorksContext GetClientDbContext(ProviderRun providerRun, DbConnection connection) => new AdventureWorksContext(providerRun, connection);

        public static Action<ProviderFixture> Configure { get; set; }
        private static bool isConfigured = false;

        private static void OnConfigure(ProviderFixture fixture)
        {
            if (isConfigured)
                return;

            // launch fixture configuration on first launch
            Configure?.Invoke(fixture);

            isConfigured = true;

        }

        /// <summary>
        /// on ctor, set the tables we want to use
        /// </summary>
        public FilterTestsBase(ProviderFixture fixture)
        {
            this.fixture = fixture;

            // Configure this tests
            OnConfigure(fixture);

            // create a test runner based on my server fixture
            this.testRunner = new TestRunner(fixture, this.fixture.ServerProvider);
        }

        /// <summary>
        /// Initialize should be always called.
        /// It creates the clients schemas and make a first sync
        /// Once Initialize() is done, both server and all clients should be equivalent
        /// </summary>
        public virtual async Task Initialize()
        {
            try
            {
                var s = new Action<SyncSchema>(c => { });

                var results = await this.testRunner.RunTestsAsync(s);

                foreach (var trr in results)
                {
                    Assert.Equal(this.fixture.RowsCount, trr.Results.TotalChangesDownloaded);
                    Assert.Equal(0, trr.Results.TotalChangesUploaded);
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                throw;
            }

        }

    
    }
}
