using System;
using System.Collections.Generic;

namespace Dotmim.Sync.Test.Misc
{
    public static class TestConfigurations
    {

        /// <summary>
        /// Always return a new list of configurations.
        /// To be sure that no tests will update a property that will be used (instead of default property) in the next test
        /// </summary>
        public static List<SyncOptions> GetOptions()
        {
            var Configurations = new List<SyncOptions>
            {
                new SyncOptions{ BatchSize = 0 },
                new SyncOptions{ BatchSize = 500 },
                new SyncOptions{ BatchSize = 500, UseBulkOperations = false },
                new SyncOptions{ BatchSize = 0, UseBulkOperations = false  }
            };

            return Configurations;
        }
    }
}