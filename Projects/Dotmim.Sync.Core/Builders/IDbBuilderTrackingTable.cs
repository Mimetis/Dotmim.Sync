

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
        Task<bool> NeedToCreateTrackingTableAsync();
        Task CreateTableAsync();
        Task DropTableAsync();
        Task RenameTableAsync(ParserName oldTableName);
        Task CreatePkAsync();
        Task CreateIndexAsync();
    }
}
