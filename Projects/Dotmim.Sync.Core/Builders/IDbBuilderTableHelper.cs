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
       Task CreateSchemaAsync(DbConnection connection, DbTransaction transaction);
       Task CreateTableAsync(DbConnection connection, DbTransaction transaction);
       Task DropTableAsync(DbConnection connection, DbTransaction transaction);
    }
}
