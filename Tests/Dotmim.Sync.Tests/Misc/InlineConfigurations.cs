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
        public static List<SyncSchema> GetSchemas()
        {
            var Configurations = new List<SyncSchema>
            {
                new SyncSchema{
                    ConflictResolutionPolicy = Enumerations.ConflictResolutionPolicy.ServerWins,
                    StoredProceduresPrefix = "",
                    StoredProceduresSuffix = "",
                    TrackingTablesPrefix = "",
                    TrackingTablesSuffix = "",
                    TriggersPrefix = "",
                    TriggersSuffix = ""
                },

                new SyncSchema{
                   ConflictResolutionPolicy = Enumerations.ConflictResolutionPolicy.ServerWins,
                   StoredProceduresPrefix = "",
                   StoredProceduresSuffix = "",
                   TrackingTablesPrefix = "",
                   TrackingTablesSuffix = "",
                   TriggersPrefix = "",
                   TriggersSuffix = ""
                }
            };

            return Configurations;
        }
    }
}