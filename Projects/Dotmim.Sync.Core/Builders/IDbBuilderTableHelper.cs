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
       //Task<bool> NeedToCreateForeignKeyConstraintsAsync(SyncRelation constraint, DbConnection connection, DbTransaction transaction);
       Task CreateSchemaAsync(DbConnection connection, DbTransaction transaction);
       Task CreateTableAsync(DbConnection connection, DbTransaction transaction);
       //Task CreatePrimaryKeyAsync(DbConnection connection, DbTransaction transaction);
       //Task CreateForeignKeyConstraintsAsync(SyncRelation constraint, DbConnection connection, DbTransaction transaction);
       Task DropTableAsync(DbConnection connection, DbTransaction transaction);
    }
}
