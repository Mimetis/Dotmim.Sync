using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class MockBuilderTrackingTableHelper : IDbBuilderTrackingTableHelper
    {
        public void CreateIndex() { }


        public void CreatePk() { }


        public void CreateTable() { }


        public void DropTable() { }


        public bool NeedToCreateTrackingTable() => true;
    }
}
