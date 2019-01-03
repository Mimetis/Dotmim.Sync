using System.Collections.Generic;

namespace Dotmim.Sync.Test.Misc
{
    public static class TestConfigurations
    {

        /// <summary>
        /// Always return a new list of configurations.
        /// To be sure that no tests will update a property that will be used (instead of default property) in the next test
        /// </summary>
        public static List<SyncConfiguration> GetConfigurations()
        {

            var Configurations = new List<SyncConfiguration>
            {
                new SyncConfiguration(),

                new SyncConfiguration
                {
                    SerializationFormat = Enumerations.SerializationFormat.Binary
                }
            };

            return Configurations;
        }
    }
}