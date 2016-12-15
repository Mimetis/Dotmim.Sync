using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync.Core.Builders
{
    /// <summary>
    /// This class will be used to generate the tracking table
    /// </summary>
    public interface IDbBuilderTrackingTableHelper
    {
        List<DmColumn> FilterColumns { get; set; }

        bool NeedToCreateTrackingTable(DbTransaction transaction, DmTable tableDescription, DbBuilderOption builderOption);

        void CreateTable(DbTransaction transaction);

        void CreatePk(DbTransaction transaction);

        void CreateIndex(DbTransaction transaction);

        void PopulateFromBaseTable(DbTransaction transaction);

        string CreateTableScriptText();

        string CreatePkScriptText();

        string CreateIndexScriptText();

        string ScriptAddFilterColumn(DmColumn filterColumn);

        string ScriptPopulateNewFilterColumnFromBaseTable(DmColumn filterColumn);

        string CreatePopulateFromBaseTableScriptText();

        void AddFilterColumn(DbTransaction transaction, DmColumn filterColumn);

        void PopulateNewFilterColumnFromBaseTable(DbTransaction transaction, DmColumn filterColumn);
    }
}
