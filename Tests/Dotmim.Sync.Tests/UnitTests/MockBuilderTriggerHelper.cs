using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class MockBuilderTriggerHelper : IDbBuilderTriggerHelper
    {
        public void AlterDeleteTrigger() { }


        public void AlterInsertTrigger() { }


        public void AlterUpdateTrigger() { }


        public void CreateDeleteTrigger() { }


        public void CreateInsertTrigger() { }


        public void CreateUpdateTrigger() { }


        public void DropDeleteTrigger()
        {
            throw new NotImplementedException();
        }

        public void DropInsertTrigger() { }


        public void DropUpdateTrigger() { }


        public bool NeedToCreateTrigger(DbTriggerType triggerType) => true;
    }
}
