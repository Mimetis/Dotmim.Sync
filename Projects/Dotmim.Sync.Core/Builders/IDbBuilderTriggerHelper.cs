using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using Dotmim.Sync.Data;

namespace Dotmim.Sync.Core.Builders
{
    /// <summary>
    /// This class is responsible to create the triggers for one particular Sync Table
    /// </summary>
    public interface IDbBuilderTriggerHelper
    {

        DmTable TableDescription { get; set; }
        DbObjectNames ObjectNames { get; set; }
        List<DmColumn> FilterColumns { get; set; }
    
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
