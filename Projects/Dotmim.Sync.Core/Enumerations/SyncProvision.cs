using System;

namespace Dotmim.Sync.Enumerations
{
    /// <summary>
    /// Gets the objects we want to provision or deprovision.
    /// </summary>
    [Flags]
    public enum SyncProvision
    {
        /// <summary>
        /// No object to provision or deprovision.
        /// </summary>
#pragma warning disable CA1008 // Enums should have zero value
        NotSet = 0,
#pragma warning restore CA1008 // Enums should have zero value

        /// <summary>
        /// Table to provision or deprovision.
        /// </summary>
        Table = 1,

        /// <summary>
        /// Tracking table to provision or deprovision.
        /// </summary>
        TrackingTable = 2,

        /// <summary>
        /// Stored procedures to provision or deprovision.
        /// </summary>
        StoredProcedures = 4,

        /// <summary>
        /// Triggers to provision or deprovision.
        /// </summary>
        Triggers = 8,

        /// <summary>
        /// Scope info to provision or deprovision.
        /// </summary>
        ScopeInfo = 16,

        /// <summary>
        /// Scope info client to provision or deprovision.
        /// </summary>
        ScopeInfoClient = 32,
    }
}