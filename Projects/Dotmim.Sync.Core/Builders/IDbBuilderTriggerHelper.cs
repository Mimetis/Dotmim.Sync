using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Builders
{
    /// <summary>
    /// This class is responsible to create the triggers for one particular Sync Table
    /// </summary>
    public interface IDbBuilderTriggerHelper
    {
        Task<bool> NeedToCreateTriggerAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction);
        Task CreateInsertTriggerAsync(DbConnection connection, DbTransaction transaction);
        Task CreateUpdateTriggerAsync(DbConnection connection, DbTransaction transaction);
        Task CreateDeleteTriggerAsync(DbConnection connection, DbTransaction transaction);
        Task DropInsertTriggerAsync(DbConnection connection, DbTransaction transaction);
        Task DropUpdateTriggerAsync(DbConnection connection, DbTransaction transaction);
        Task DropDeleteTriggerAsync(DbConnection connection, DbTransaction transaction);
        Task AlterInsertTriggerAsync(DbConnection connection, DbTransaction transaction);
        Task AlterUpdateTriggerAsync(DbConnection connection, DbTransaction transaction);
        Task AlterDeleteTriggerAsync(DbConnection connection, DbTransaction transaction);
    }
}
