

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
        bool NeedToCreateTrackingTable();
        void CreateTable();
        void DropTable();
        void CreatePk();
        void CreateIndex();
    }
}
