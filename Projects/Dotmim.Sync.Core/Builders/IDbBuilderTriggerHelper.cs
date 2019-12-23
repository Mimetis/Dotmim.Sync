using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using Dotmim.Sync.Data;
using Dotmim.Sync.Filter;

namespace Dotmim.Sync.Builders
{
    /// <summary>
    /// This class is responsible to create the triggers for one particular Sync Table
    /// </summary>
    public interface IDbBuilderTriggerHelper
    {
        IEnumerable<SyncFilter> Filters { get; set; }
    
        bool NeedToCreateTrigger(DbTriggerType triggerType);
        void CreateInsertTrigger();
        void CreateUpdateTrigger();
        void CreateDeleteTrigger();
        string CreateInsertTriggerScriptText();
        string CreateUpdateTriggerScriptText();
        string CreateDeleteTriggerScriptText();
        void DropInsertTrigger();
        void DropUpdateTrigger();
        void DropDeleteTrigger();
        string DropInsertTriggerScriptText();
        string DropUpdateTriggerScriptText();
        string DropDeleteTriggerScriptText();
        void AlterInsertTrigger();
        void AlterUpdateTrigger();
        void AlterDeleteTrigger();
        string AlterInsertTriggerScriptText();
        string AlterUpdateTriggerScriptText();
        string AlterDeleteTriggerScriptText();
    }
}
