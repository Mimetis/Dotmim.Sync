using Dotmim.Sync.Core.Builders;
using System;
using System.Collections.Generic;
using System.Text;
using Dotmim.Sync.Data;
using System.Data.Common;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlBuilderProcedure : IDbBuilderProcedureHelper
    {
        private DmTable table;

        public SqlBuilderProcedure(DmTable tableDescription)
        {
            this.table = tableDescription;
        }
        public List<DmColumn> FilterColumns { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public List<DmColumn> FilterParameters { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public void CreateBulkDelete(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public string CreateBulkDeleteScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public void CreateBulkInsert(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public string CreateBulkInsertScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public void CreateBulkUpdate(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public string CreateBulkUpdateScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public void CreateDelete(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public void CreateDeleteMetadata(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public string CreateDeleteMetadataScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public string CreateDeleteScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public void CreateInsert(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public void CreateInsertMetadata(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public string CreateInsertMetadataScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public string CreateInsertScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public void CreateSelectRow(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public string CreateSelectRowScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public void CreateTVPType(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public string CreateTVPTypeScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public void CreateUpdate(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public void CreateUpdateMetadata(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public string CreateUpdateMetadataScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }

        public string CreateUpdateScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            throw new NotImplementedException();
        }
    }
}
