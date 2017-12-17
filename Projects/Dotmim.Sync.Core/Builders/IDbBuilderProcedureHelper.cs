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
        void DropSelectRow();
        void DropSelectIncrementalChanges();
        void DropInsert();
        void DropUpdate();
        void DropDelete();
        void DropInsertMetadata();
        void DropUpdateMetadata();
        void DropDeleteMetadata();
        void DropTVPType();
        void DropBulkInsert();
        void DropBulkUpdate();
        void DropBulkDelete();
        void DropReset();
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
        String DropSelectRowScriptText();
        String DropSelectIncrementalChangesScriptText();
        String DropInsertScriptText();
        String DropUpdateScriptText();
        String DropDeleteScriptText();
        String DropInsertMetadataScriptText();
        String DropUpdateMetadataScriptText();
        String DropDeleteMetadataScriptText();
        String DropTVPTypeScriptText();
        String DropBulkInsertScriptText();
        String DropBulkUpdateScriptText();
        String DropBulkDeleteScriptText();
        String DropResetScriptText();

    }
}
