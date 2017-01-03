using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using Dotmim.Sync.Data;

namespace Dotmim.Sync.Core.Builders
{
    /// <summary>
    /// This class is responsible to create a stored proc for one particular sync table
    /// </summary>
    public interface IDbBuilderProcedureHelper
    {
       DmTable TableDescription { get; set; }
       DbObjectNames ObjectNames { get; set; }
       List<DmColumn> FilterColumns { get; set; }
       List<DmColumn> FilterParameters { get; set; }

        bool NeedToCreateProcedure(string procedureName, DbBuilderOption builderOption);
        bool NeedToCreateType(string typeName, DbBuilderOption builderOption);
        void CreateSelectRow(string procedureName);
        void CreateSelectIncrementalChanges(string procedureName);
        void CreateInsert(string procedureName);
        void CreateUpdate(string procedureName);
        void CreateDelete(string procedureName);
        void CreateInsertMetadata(string procedureName);
        void CreateUpdateMetadata(string procedureName);
        void CreateDeleteMetadata(string procedureName);
        void CreateTVPType(string bulkTypeName);
        void CreateBulkInsert(string procedureName);
        void CreateBulkUpdate(string procedureName);
        void CreateBulkDelete(string procedureName);
        String CreateSelectRowScriptText(string procedureName);
        String CreateSelectIncrementalChangesScriptText(string procedureName);
        String CreateInsertScriptText(string procedureName);
        String CreateUpdateScriptText(string procedureName);
        String CreateDeleteScriptText(string procedureName);
        String CreateInsertMetadataScriptText(string procedureName);
        String CreateUpdateMetadataScriptText(string procedureName);
        String CreateDeleteMetadataScriptText(string procedureName);
        String CreateTVPTypeScriptText(string bulkTypeName);
        String CreateBulkInsertScriptText(string procedureName);
        String CreateBulkUpdateScriptText(string procedureName);
        String CreateBulkDeleteScriptText(string procedureName);
    }
}
