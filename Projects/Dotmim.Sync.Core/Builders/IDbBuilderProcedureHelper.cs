using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using Dotmim.Sync.Data;
using Dotmim.Sync.Filter;

namespace Dotmim.Sync.Builders
{
    /// <summary>
    /// This class is responsible to create a stored proc for one particular sync table
    /// </summary>
    public interface IDbBuilderProcedureHelper
    {
        FilterClauseCollection Filters { get; set; }

        bool NeedToCreateProcedure(DbCommandType commandName);
        bool NeedToCreateType(DbCommandType typeName);
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
        void CreateReset();
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
        String CreateResetScriptText();

    }
}
