using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Builders
{
    /// <summary>
    /// This class is responsible to create a stored proc for one particular sync table
    /// </summary>
    public interface IDbBuilderProcedureHelper
    {
        Task<bool> NeedToCreateProcedureAsync(DbCommandType commandName);
        Task<bool> NeedToCreateTypeAsync(DbCommandType typeName);
        Task CreateSelectRowAsync();
        Task CreateSelectIncrementalChangesAsync(SyncFilter filter);
        Task CreateSelectInitializedChangesAsync(SyncFilter filter);
        Task CreateUpdateAsync(bool hasMutableColumns);
        Task CreateDeleteAsync();
        Task CreateDeleteMetadataAsync();
        Task CreateTVPTypeAsync();
        Task CreateBulkUpdateAsync(bool hasMutableColumns);
        Task CreateBulkDeleteAsync();
        Task CreateResetAsync();
        Task DropSelectRowAsync();
        Task DropSelectIncrementalChangesAsync(SyncFilter filter);
        Task DropSelectInitializedChangesAsync(SyncFilter filter);
        Task DropUpdateAsync();
        Task DropDeleteAsync();
        Task DropDeleteMetadataAsync();
        Task DropTVPTypeAsync();
        Task DropBulkUpdateAsync();
        Task DropBulkDeleteAsync();
        Task DropResetAsync();
    }
}
