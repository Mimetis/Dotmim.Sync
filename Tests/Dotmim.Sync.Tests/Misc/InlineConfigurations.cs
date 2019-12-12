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
                // First options with batch enabled
                new SyncOptions{ BatchSize = 1000 },

                // Second options without batch enabled
                new SyncOptions{ BatchSize = 0 }
            };

            return Configurations;
        }
    }
}