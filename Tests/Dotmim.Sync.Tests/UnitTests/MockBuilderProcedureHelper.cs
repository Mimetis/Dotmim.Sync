using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class MockBuilderProcedureHelper : IDbBuilderProcedureHelper
    {
        public void CreateBulkDelete() { }

        public void CreateBulkUpdate(bool hasMutableColumns) { }


        public void CreateDelete() { }


        public void CreateDeleteMetadata() { }


        public void CreateReset() { }


        public void CreateSelectIncrementalChanges(SyncFilter filter) { }


        public void CreateSelectInitializedChanges(SyncFilter filter) { }


        public void CreateSelectRow() { }


        public void CreateTVPType() { }


        public void CreateUpdate(bool hasMutableColumns) { }


        public void DropBulkDelete() { }


        public void DropBulkUpdate() { }


        public void DropDelete() { }


        public void DropDeleteMetadata() { }


        public void DropReset() { }


        public void DropSelectIncrementalChanges(SyncFilter filter) { }


        public void DropSelectInitializedChanges(SyncFilter filter) { }


        public void DropSelectRow() { }


        public void DropTVPType() { }


        public void DropUpdate() { }


        public bool NeedToCreateProcedure(DbCommandType commandName) => true;


        public bool NeedToCreateType(DbCommandType typeName) => true;

    }
}
