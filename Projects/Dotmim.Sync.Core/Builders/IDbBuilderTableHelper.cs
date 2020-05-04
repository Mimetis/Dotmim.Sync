using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Builders
{

    /// <summary>
    /// Helper for create a table
    /// </summary>
    public interface IDbBuilderTableHelper
    {
       Task<bool> NeedToCreateTableAsync();
       Task<bool> NeedToCreateSchemaAsync();
       Task<bool> NeedToCreateForeignKeyConstraintsAsync(SyncRelation constraint);
       Task CreateSchemaAsync();
       Task CreateTableAsync();
       Task CreatePrimaryKeyAsync();
       Task CreateForeignKeyConstraintsAsync(SyncRelation constraint);
       Task DropTableAsync();
    }
}
