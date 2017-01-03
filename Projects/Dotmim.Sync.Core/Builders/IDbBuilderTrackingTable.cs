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
        DmTable TableDescription { get; set; }
        List<DmColumn> FilterColumns { get; set; }
        bool NeedToCreateTrackingTable(DbBuilderOption builderOption);
        void CreateTable();
        void CreatePk();
        void CreateIndex();
        void PopulateFromBaseTable();
        string CreateTableScriptText();
        string CreatePkScriptText();
        string CreateIndexScriptText();
        string ScriptAddFilterColumn(DmColumn filterColumn);
        string ScriptPopulateNewFilterColumnFromBaseTable(DmColumn filterColumn);
        string CreatePopulateFromBaseTableScriptText();
        void AddFilterColumn(DmColumn filterColumn);
        void PopulateNewFilterColumnFromBaseTable(DmColumn filterColumn);
    }
}
