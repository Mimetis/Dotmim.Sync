
//using System.Collections.Generic;
//using System.Threading.Tasks;

//namespace Dotmim.Sync.Manager
//{
//    public interface IDbTableManager
//    {
//        /// <summary>
//        /// Sets the current tableName
//        /// </summary>
//        string TableName { set; }
//        string SchemaName { set; }


//        /// <summary>
//        /// Get the table from data source.
//        /// The main purpose of this call is to be sure the table exists
//        /// </summary>
//        /// <returns></returns>
//        Task<SyncTable> GetTableAsync();

//        /// <summary>
//        /// Gets a columns list from the datastore
//        /// </summary>
//        Task<IEnumerable<SyncColumn>> GetColumnsAsync();

//        /// <summary>
//        /// Gets all relations from a current table. If composite, must be ordered
//        /// </summary>
//        Task<IEnumerable<DbRelationDefinition>> GetRelationsAsync();

//        /// <summary>
//        /// Get all primary keys. If composite, must be ordered
//        /// </summary>
//        Task<IEnumerable<SyncColumn>> GetPrimaryKeysAsync();

//    }
//}
