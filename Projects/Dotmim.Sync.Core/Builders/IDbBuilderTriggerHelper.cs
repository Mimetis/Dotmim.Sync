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
        List<DmColumn> FilterColumns { get; set; }

        void CreateInsertTrigger(DbTransaction transaction, DbBuilderOption builderOption);

        void CreateUpdateTrigger(DbTransaction transaction, DbBuilderOption builderOption);

        void CreateDeleteTrigger(DbTransaction transaction, DbBuilderOption builderOption);

        string AlterInsertTriggerScriptText();

        string AlterUpdateTriggerScriptText();

        string AlterDeleteTriggerScriptText();

        string CreateInsertTriggerScriptText(DbTransaction transaction, DbBuilderOption builderOption);

        string CreateUpdateTriggerScriptText(DbTransaction transaction, DbBuilderOption builderOption);

        string CreateDeleteTriggerScriptText(DbTransaction transaction, DbBuilderOption builderOption);

        void AlterInsertTrigger(DbTransaction transaction);

        void AlterUpdateTrigger(DbTransaction transaction);

        void AlterDeleteTrigger(DbTransaction transaction);
    }
}
