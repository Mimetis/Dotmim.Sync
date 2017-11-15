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
        bool NeedToCreateTable(DbBuilderOption builderOption);
        bool NeedToCreateSchema(DbBuilderOption builderOption);
        bool NeedToCreateForeignKeyConstraints(DmRelation constraint, DbBuilderOption builderOption);
        void CreateSchema();
        void CreateTable();
        void CreatePrimaryKey();
        void CreateForeignKeyConstraints(DmRelation constraint);
        string CreateSchemaScriptText();
        string CreateTableScriptText();
        string CreatePrimaryKeyScriptText();
        string CreateForeignKeyConstraintsScriptText(DmRelation constraint);
    }
}
