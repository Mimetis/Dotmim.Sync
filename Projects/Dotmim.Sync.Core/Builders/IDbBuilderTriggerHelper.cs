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
        FilterClauseCollection Filters { get; set; }
    
        bool NeedToCreateTrigger(DbTriggerType triggerType, DbBuilderOption builderOption);
        void CreateInsertTrigger();
        void CreateUpdateTrigger();
        void CreateDeleteTrigger();
        string AlterInsertTriggerScriptText();
        string AlterUpdateTriggerScriptText();
        string AlterDeleteTriggerScriptText();
        string CreateInsertTriggerScriptText();
        string CreateUpdateTriggerScriptText();
        string CreateDeleteTriggerScriptText();
        void AlterInsertTrigger();
        void AlterUpdateTrigger();
        void AlterDeleteTrigger();
    }
}
