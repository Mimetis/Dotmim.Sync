using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;



namespace Dotmim.Sync.Builders
{
    /// <summary>
    /// This class is responsible to create the triggers for one particular Sync Table
    /// </summary>
    public interface IDbBuilderTriggerHelper
    {
   
        bool NeedToCreateTrigger(DbTriggerType triggerType);
        void CreateInsertTrigger();
        void CreateUpdateTrigger();
        void CreateDeleteTrigger();
        void DropInsertTrigger();
        void DropUpdateTrigger();
        void DropDeleteTrigger();
        void AlterInsertTrigger();
        void AlterUpdateTrigger();
        void AlterDeleteTrigger();
    }
}
