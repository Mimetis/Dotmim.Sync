using Dotmim.Sync.Core.Builders;
using System;
using System.Collections.Generic;
using System.Text;
using Dotmim.Sync.Data;
using System.Data.Common;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlBuilderTriggerHelper : IDbBuilderTriggerHelper
    {

        private DmTable table;

        public SqlBuilderTriggerHelper(DmTable tableDescription)
        {
            this.table = tableDescription;
        }

        public List<DmColumn> FilterColumns { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public void AlterDeleteTrigger(DbTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public string AlterDeleteTriggerScriptText()
        {
            throw new NotImplementedException();
        }

        public void AlterInsertTrigger(DbTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public string AlterInsertTriggerScriptText()
        {
            throw new NotImplementedException();
        }

        public void AlterUpdateTrigger(DbTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public string AlterUpdateTriggerScriptText()
        {
            throw new NotImplementedException();
        }

        public void CreateDeleteTrigger(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public string CreateDeleteTriggerScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public void CreateInsertTrigger(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public string CreateInsertTriggerScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public void CreateUpdateTrigger(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public string CreateUpdateTriggerScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }
    }
}
