using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class MockBuilderTableHelper : IDbBuilderTableHelper
    {
        public void CreateForeignKeyConstraints(SyncRelation constraint) { }


        public void CreatePrimaryKey() { }


        public void CreateSchema() { }


        public void CreateTable() { }


        public void DropTable() { }


        public bool NeedToCreateForeignKeyConstraints(SyncRelation constraint) => true;

        public bool NeedToCreateSchema() => true;

        public bool NeedToCreateTable() => true;
    }
}
