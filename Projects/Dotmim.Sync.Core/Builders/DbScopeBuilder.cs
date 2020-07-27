using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync.Builders
{
    public abstract class DbScopeBuilder 
    {
        public abstract IDbScopeInfoBuilder CreateScopeInfoBuilder(string scopeTableName);
    }
}
