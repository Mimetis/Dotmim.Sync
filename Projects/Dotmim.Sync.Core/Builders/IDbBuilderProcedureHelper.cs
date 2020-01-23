using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

using Dotmim.Sync.Filter;

namespace Dotmim.Sync.Builders
{
    /// <summary>
    /// This class is responsible to create a stored proc for one particular sync table
    /// </summary>
    public interface IDbBuilderProcedureHelper
    {
        SyncFilters Filters { get; set; }

        bool NeedToCreateProcedure(DbCommandType commandName);
        bool NeedToCreateType(DbCommandType typeName);
        void CreateSelectRow();
        void CreateSelectIncrementalChanges();
        void CreateSelectInitializedChanges();
        void CreateUpdate(bool hasMutableColumns);
        void CreateDelete();
        void CreateUpdateMetadata();
        void CreateDeleteMetadata();
        void CreateTVPType();
        void CreateBulkUpdate(bool hasMutableColumns);
        void CreateBulkDelete();
        void CreateReset();
        void DropSelectRow();
        void DropSelectIncrementalChanges();
        void DropSelectInitializedChanges();
        void DropUpdate();
        void DropDelete();
        void DropUpdateMetadata();
        void DropDeleteMetadata();
        void DropTVPType();
        void DropBulkUpdate();
        void DropBulkDelete();
        void DropReset();
        String CreateSelectRowScriptText();
        String CreateSelectIncrementalChangesScriptText();
        String CreateSelectInitializedChangesScriptText();
        String CreateUpdateScriptText(bool hasMutableColumns);
        String CreateDeleteScriptText();
        String CreateUpdateMetadataScriptText();
        String CreateDeleteMetadataScriptText();
        String CreateTVPTypeScriptText();
        String CreateBulkUpdateScriptText(bool hasMutableColumns);
        String CreateBulkDeleteScriptText();
        String CreateResetScriptText();
        String DropSelectRowScriptText();
        String DropSelectIncrementalChangesScriptText();
        String DropUpdateScriptText();
        String DropDeleteScriptText();
        String DropUpdateMetadataScriptText();
        String DropDeleteMetadataScriptText();
        String DropTVPTypeScriptText();
        String DropBulkUpdateScriptText();
        String DropBulkDeleteScriptText();
        String DropResetScriptText();

    }
}
