using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using Dotmim.Sync.Data;
using Dotmim.Sync.Core.Filter;

namespace Dotmim.Sync.Core.Builders
{
    /// <summary>
    /// This class is responsible to create a stored proc for one particular sync table
    /// </summary>
    public interface IDbBuilderProcedureHelper
    {
        FilterClauseCollection Filters { get; set; }

        bool NeedToCreateProcedure(DbCommandType commandName, DbBuilderOption builderOption);
        bool NeedToCreateType(DbCommandType typeName, DbBuilderOption builderOption);
        void CreateSelectRow();
        void CreateSelectIncrementalChanges();
        void CreateInsert();
        void CreateUpdate();
        void CreateDelete();
        void CreateInsertMetadata();
        void CreateUpdateMetadata();
        void CreateDeleteMetadata();
        void CreateTVPType();
        void CreateBulkInsert();
        void CreateBulkUpdate();
        void CreateBulkDelete();
        String CreateSelectRowScriptText();
        String CreateSelectIncrementalChangesScriptText();
        String CreateInsertScriptText();
        String CreateUpdateScriptText();
        String CreateDeleteScriptText();
        String CreateInsertMetadataScriptText();
        String CreateUpdateMetadataScriptText();
        String CreateDeleteMetadataScriptText();
        String CreateTVPTypeScriptText();
        String CreateBulkInsertScriptText();
        String CreateBulkUpdateScriptText();
        String CreateBulkDeleteScriptText();
    }
}
