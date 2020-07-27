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
        Task<bool> NeedToCreateProcedureAsync(DbCommandType commandName, DbConnection connection, DbTransaction transaction);
        Task<bool> NeedToCreateTypeAsync(DbCommandType typeName, DbConnection connection, DbTransaction transaction);
        Task CreateSelectRowAsync(DbConnection connection, DbTransaction transaction);
        Task CreateSelectIncrementalChangesAsync(SyncFilter filter, DbConnection connection, DbTransaction transaction);
        Task CreateSelectInitializedChangesAsync(SyncFilter filter, DbConnection connection, DbTransaction transaction);
        Task CreateUpdateAsync(bool hasMutableColumns, DbConnection connection, DbTransaction transaction);
        Task CreateDeleteAsync(DbConnection connection, DbTransaction transaction);
        Task CreateDeleteMetadataAsync(DbConnection connection, DbTransaction transaction);
        Task CreateTVPTypeAsync(DbConnection connection, DbTransaction transaction);
        Task CreateBulkUpdateAsync(bool hasMutableColumns, DbConnection connection, DbTransaction transaction);
        Task CreateBulkDeleteAsync(DbConnection connection, DbTransaction transaction);
        Task CreateResetAsync(DbConnection connection, DbTransaction transaction);
        Task DropSelectRowAsync(DbConnection connection, DbTransaction transaction);
        Task DropSelectIncrementalChangesAsync(SyncFilter filter, DbConnection connection, DbTransaction transaction);
        Task DropSelectInitializedChangesAsync(SyncFilter filter, DbConnection connection, DbTransaction transaction);
        Task DropUpdateAsync(DbConnection connection, DbTransaction transaction);
        Task DropDeleteAsync(DbConnection connection, DbTransaction transaction);
        Task DropDeleteMetadataAsync(DbConnection connection, DbTransaction transaction);
        Task DropTVPTypeAsync(DbConnection connection, DbTransaction transaction);
        Task DropBulkUpdateAsync(DbConnection connection, DbTransaction transaction);
        Task DropBulkDeleteAsync(DbConnection connection, DbTransaction transaction);
        Task DropResetAsync(DbConnection connection, DbTransaction transaction);
    }
}
