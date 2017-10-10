using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

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
        List<DmColumn> GetTableDefinition();

        /// <summary>
        /// Gets all relations from a current table
        /// </summary>
        List<DbRelationDefinition> GetTableRelations();


        List<string> GetTablePrimaryKeys();

    }
}
