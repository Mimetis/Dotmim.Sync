using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Builders
{

    /// <summary>
    /// Option for creation of the database both server or client
    /// On Server DB must exist, but eventually not the tracking table 
    /// So for server we should have something lik SkipCreateSchema & CreateOrUseTrackingTables
    /// For Client, DB exist,but could be empty, we should have something like (CreateOrUseExistingSchema & CreateOrUseTrackingTables)
    /// 
    /// </summary>
    [Flags]
    public enum DbBuilderOption
    {
        /// <summary>
        /// Creates the object.
        /// </summary>
		CreateOrUseExistingSchema = 1,

        /// <summary>
        /// Should Create tracking tables
        /// </summary>
        CreateOrUseExistingTrackingTables = 2,

        /// <summary>
        /// Do not create the object. Assuming the schema already exist
        /// </summary>
        UseExistingSchema = 4,

        /// <summary>
        /// Do not create the tracking tables. Assuming tracking tables already exist
        /// </summary>
        UseExistingTrackingTables = 8,

    }
}
