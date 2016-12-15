using Dotmim.Sync.Core.Builders;
using System;
using System.Collections.Generic;
using System.Text;
using Dotmim.Sync.Data;
using System.Data.Common;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlBuilderTable : IDbBuilderTableHelper
    {
        private DmTable table;

        public SqlBuilderTable(DmTable tableDescription)
        {
            this.table = tableDescription;
        }
        public void CreateForeignKeyConstraints(DbTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public bool CreateForeignKeyConstraintsScriptText()
        {
            throw new NotImplementedException();
        }

        public void CreatePk(DbTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public bool CreatePkScriptText()
        {
            throw new NotImplementedException();
        }

        public void CreateTable(DbTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public string CreateTableScriptText()
        {
            throw new NotImplementedException();
        }

        public List<string> GetColumnForTable(DbTransaction transaction, string tableName)
        {
            throw new NotImplementedException();
        }

        public bool NeedToCreateTable(DbTransaction transaction, DmTable tableDescription)
        {
            throw new NotImplementedException();
        }
    }
}
