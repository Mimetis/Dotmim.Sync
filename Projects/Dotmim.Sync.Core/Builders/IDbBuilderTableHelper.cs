using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using Dotmim.Sync.Data;

namespace Dotmim.Sync.Builders
{

    /// <summary>
    /// Helper for create a table
    /// </summary>
    public interface IDbBuilderTableHelper
    {
        bool NeedToCreateTable();
        bool NeedToCreateSchema();
        bool NeedToCreateForeignKeyConstraints(SyncRelation constraint);
        void CreateSchema();
        void CreateTable();
        void CreatePrimaryKey();
        void CreateForeignKeyConstraints(SyncRelation constraint);
        void DropTable();
        string CreateSchemaScriptText();
        string CreateTableScriptText();
        string CreatePrimaryKeyScriptText();
        string CreateForeignKeyConstraintsScriptText(SyncRelation constraint);
        string DropTableScriptText();
    }
}
