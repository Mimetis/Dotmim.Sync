using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using Dotmim.Sync.Data;

namespace Dotmim.Sync.Core.Builders
{

    /// <summary>
    /// Helper for create a table
    /// </summary>
    public interface IDbBuilderTableHelper
    {

        /// <summary>
        /// Use the database engine to get a list of columns for a table name.
        /// Actually, the transaction is open and the connection is open.
        /// </summary>
        List<String> GetColumnForTable(DbTransaction transaction, string tableName);

        bool NeedToCreateTable(DbTransaction transaction, DmTable tableDescription, DbBuilderOption builderOption);

        void CreateTable(DbTransaction transaction, DbBuilderOption builderOption);

        void CreatePk(DbTransaction transaction, DbBuilderOption builderOption);

        void CreateForeignKeyConstraints(DbTransaction transaction, DbBuilderOption builderOption);

        string CreateTableScriptText(DbTransaction transaction, DbBuilderOption builderOption);

        string CreatePkScriptText(DbTransaction transaction, DbBuilderOption builderOption);

        string CreateForeignKeyConstraintsScriptText(DbTransaction transaction, DbBuilderOption builderOption);
    }
}
