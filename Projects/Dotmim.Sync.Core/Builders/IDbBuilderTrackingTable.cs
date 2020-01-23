using Dotmim.Sync.Filter;

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync.Builders
{
    /// <summary>
    /// This class will be used to generate the tracking table
    /// </summary>
    public interface IDbBuilderTrackingTableHelper
    {
        IEnumerable<SyncFilter> Filters { get; set; }
        bool NeedToCreateTrackingTable();
        void CreateTable();
        void DropTable();
        void CreatePk();
        void CreateIndex();
        void PopulateFromBaseTable();
        string CreateTableScriptText();
        string DropTableScriptText();
        string CreatePkScriptText();
        string CreateIndexScriptText();
        string ScriptAddFilterColumn(SyncColumn filterColumn);
        string CreatePopulateFromBaseTableScriptText();
        void AddFilterColumn(SyncColumn filterColumn);
    }
}
