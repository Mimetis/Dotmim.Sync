
using System.Collections.Generic;

namespace Dotmim.Sync.Manager
{
    public interface IDbTableManager
    {
        /// <summary>
        /// Sets the current tableName
        /// </summary>
        string TableName { set; }
        string SchemaName { set; }


        /// <summary>
        /// Get the table from data source.
        /// The main purpose of this call is to be sure the table exists
        /// </summary>
        /// <returns></returns>
        SyncTable GetTable();

        /// <summary>
        /// Gets a columns list from the datastore
        /// </summary>
        IEnumerable<SyncColumn> GetColumns();

        /// <summary>
        /// Gets all relations from a current table. If composite, must be ordered
        /// </summary>
        IEnumerable<DbRelationDefinition> GetRelations();

        /// <summary>
        /// Get all primary keys. If composite, must be ordered
        /// </summary>
        IEnumerable<SyncColumn> GetPrimaryKeys();

    }
}
