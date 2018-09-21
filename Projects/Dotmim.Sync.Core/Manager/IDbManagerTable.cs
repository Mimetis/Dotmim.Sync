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

        /// <summary>
        /// Gets a columns list from the datastore
        /// </summary>
        IEnumerable<DmColumn> GetTableDefinition();

        /// <summary>
        /// Gets all relations from a current table
        /// </summary>
        IEnumerable<DbRelationDefinition> GetTableRelations();


        IEnumerable<string> GetTablePrimaryKeys();

    }
}
