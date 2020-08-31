

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Builders
{
    /// <summary>
    /// This class will be used to generate the tracking table
    /// </summary>
    public interface IDbBuilderTrackingTableHelper
    {
        Task<bool> NeedToCreateTrackingTableAsync(DbConnection connection, DbTransaction transaction);
        Task CreateTableAsync(DbConnection connection, DbTransaction transaction);
        Task DropTableAsync(DbConnection connection, DbTransaction transaction);
        Task RenameTableAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction);
        Task CreatePkAsync(DbConnection connection, DbTransaction transaction);
        Task CreateIndexAsync(DbConnection connection, DbTransaction transaction);
    }
}
