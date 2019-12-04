using Dotmim.Sync.Data;
using System.Collections.Generic;

namespace Dotmim.Sync.Manager
{
    public interface IDbManagerTable
    {
        /// <summary>
        /// Sets the current tableName
        /// </summary>
        string TableName { set; }
        string SchemaName { set; }

        /// <summary>
        /// Gets a columns list from the datastore
        /// </summary>
        IEnumerable<DmColumn> GetTableDefinition();

        /// <summary>
        /// Gets all relations from a current table. If composite, must be ordered
        /// </summary>
        IEnumerable<DbRelationDefinition> GetTableRelations();

        /// <summary>
        /// Get all primary keys. If composite, must be ordered
        /// </summary>
        IEnumerable<DmColumn> GetTablePrimaryKeys();

    }
}
