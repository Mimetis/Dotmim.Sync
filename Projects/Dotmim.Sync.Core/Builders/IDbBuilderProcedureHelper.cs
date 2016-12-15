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

        List<DmColumn> FilterColumns { get; set; }
        List<DmColumn> FilterParameters { get; set; }

        void CreateSelectRow(DbTransaction transaction, DbBuilderOption builderOption);

        void CreateInsert(DbTransaction transaction, DbBuilderOption builderOption);

        void CreateUpdate(DbTransaction transaction, DbBuilderOption builderOption);

        void CreateDelete(DbTransaction transaction, DbBuilderOption builderOption);

        void CreateInsertMetadata(DbTransaction transaction, DbBuilderOption builderOption);

        void CreateUpdateMetadata(DbTransaction transaction, DbBuilderOption builderOption);

        void CreateDeleteMetadata(DbTransaction transaction, DbBuilderOption builderOption);

        void CreateTVPType(DbTransaction transaction, DbBuilderOption builderOption);

        void CreateBulkInsert(DbTransaction transaction, DbBuilderOption builderOption);

        void CreateBulkUpdate(DbTransaction transaction, DbBuilderOption builderOption);

        void CreateBulkDelete(DbTransaction transaction, DbBuilderOption builderOption);

        String CreateSelectRowScriptText(DbTransaction transaction, DbBuilderOption builderOption);

        String CreateInsertScriptText(DbTransaction transaction, DbBuilderOption builderOption);

        String CreateUpdateScriptText(DbTransaction transaction, DbBuilderOption builderOption);

        String CreateDeleteScriptText(DbTransaction transaction, DbBuilderOption builderOption);

        String CreateInsertMetadataScriptText(DbTransaction transaction, DbBuilderOption builderOption);

        String CreateUpdateMetadataScriptText(DbTransaction transaction, DbBuilderOption builderOption);

        String CreateDeleteMetadataScriptText(DbTransaction transaction, DbBuilderOption builderOption);

        String CreateTVPTypeScriptText(DbTransaction transaction, DbBuilderOption builderOption);

        String CreateBulkInsertScriptText(DbTransaction transaction, DbBuilderOption builderOption);

        String CreateBulkUpdateScriptText(DbTransaction transaction, DbBuilderOption builderOption);

        String CreateBulkDeleteScriptText(DbTransaction transaction, DbBuilderOption builderOption);
    }
}
