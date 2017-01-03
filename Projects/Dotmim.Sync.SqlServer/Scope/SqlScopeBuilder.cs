using Dotmim.Sync.Core.Scope;
using System;
using System.Collections.Generic;
using System.Text;
using System.Data.Common;
using System.Data.SqlClient;

namespace Dotmim.Sync.SqlServer.Scope
{
    public class SqlScopeBuilder : DbScopeBuilder
    {
    
        public override IDbScopeConfigBuilder CreateScopeConfigBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return null;
            //return new SqlScopeConfigBuilder(connection, transaction);
        }

        public override IDbScopeInfoBuilder CreateScopeInfoBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return (IDbScopeInfoBuilder)(new SqlScopeInfoBuilder(connection, transaction));
        }
    }
}
