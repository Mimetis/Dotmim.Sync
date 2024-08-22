using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Builders
{
    /// <summary>
    /// All commands type for the scope info tables used during sync.
    /// </summary>
    public enum DbScopeCommandType
    {
        /// <summary>
        /// Exists scope info table command.
        /// </summary>
        ExistsScopeInfoTable,

        /// <summary>
        /// Exists scope info client table command.
        /// </summary>
        ExistsScopeInfoClientTable,

        /// <summary>
        /// Create scope info table command.
        /// </summary>
        CreateScopeInfoTable,

        /// <summary>
        /// Create scope info client table command.
        /// </summary>
        CreateScopeInfoClientTable,

        /// <summary>
        /// Drop scope info table command.
        /// </summary>
        DropScopeInfoTable,

        /// <summary>
        /// Drop scope info client table command.
        /// </summary>
        DropScopeInfoClientTable,

        /// <summary>
        /// Get all scope info command.
        /// </summary>
        GetAllScopeInfos,

        /// <summary>
        /// Get all scope info clients command.
        /// </summary>
        GetAllScopeInfoClients,

        /// <summary>
        /// Get scope info command.
        /// </summary>
        GetScopeInfo,

        /// <summary>
        /// Get scope info client command.
        /// </summary>
        GetScopeInfoClient,

        /// <summary>
        /// Insert scope info command.
        /// </summary>
        InsertScopeInfo,

        /// <summary>
        /// Insert scope info client command.
        /// </summary>
        InsertScopeInfoClient,

        /// <summary>
        /// Update scope info command.
        /// </summary>
        UpdateScopeInfo,

        /// <summary>
        /// Update scope info client command.
        /// </summary>
        UpdateScopeInfoClient,

        /// <summary>
        /// Delete scope info command.
        /// </summary>
        DeleteScopeInfo,

        /// <summary>
        /// Delete scope info client command.
        /// </summary>
        DeleteScopeInfoClient,

        /// <summary>
        /// Exist scope info command.
        /// </summary>
        ExistScopeInfo,

        /// <summary>
        /// Exist scope info client command.
        /// </summary>
        ExistScopeInfoClient,

        /// <summary>
        /// Get local timestamp command.
        /// </summary>
        GetLocalTimestamp,
    }
}