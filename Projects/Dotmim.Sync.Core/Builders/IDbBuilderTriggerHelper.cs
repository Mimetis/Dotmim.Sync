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
        Task<bool> NeedToCreateTriggerAsync(DbTriggerType triggerType);
        Task CreateInsertTriggerAsync();
        Task CreateUpdateTriggerAsync();
        Task CreateDeleteTriggerAsync();
        Task DropInsertTriggerAsync();
        Task DropUpdateTriggerAsync();
        Task DropDeleteTriggerAsync();
        Task AlterInsertTriggerAsync();
        Task AlterUpdateTriggerAsync();
        Task AlterDeleteTriggerAsync();
    }
}
