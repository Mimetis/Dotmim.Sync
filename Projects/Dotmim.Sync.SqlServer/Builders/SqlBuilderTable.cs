using Dotmim.Sync.Core.Builders;
using System;
using System.Collections.Generic;
using System.Text;
using Dotmim.Sync.Data;
using System.Data.Common;
using Dotmim.Sync.Core.Common;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlBuilderTable : IDbBuilderTableHelper
    {
        private DmTable table;
        private ObjectNameParser originalTableName;

        public SqlBuilderTable(DmTable tableDescription)
        {
            this.table = tableDescription;
            string tableAndPrefixName = String.IsNullOrWhiteSpace(this.table.Prefix) ? this.table.TableName : $"{this.table.Prefix}.{this.table.TableName}";
            this.originalTableName = new ObjectNameParser(tableAndPrefixName, "[", "]");
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


        private string CreateTableCommandText()
        {
            return null;
            //StringBuilder stringBuilder = new StringBuilder($"CREATE TABLE {this._tableDesc.LocalName} (");
            //string empty = string.Empty;
            //foreach (DbSyncColumnDescription column in this._tableDesc.Columns)
            //{
            //    stringBuilder.Append(string.Concat(empty, column.DefinitionString));
            //    empty = ", ";
            //}
            //stringBuilder.Append(")");
            //return stringBuilder.ToString();
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
