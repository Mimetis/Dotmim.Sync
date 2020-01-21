using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync.MySql.Builders
{
    public class MySqlBuilder : DbBuilder
    {
        public override void EnsureDatabase(DbConnection connection, DbTransaction transaction = null)
        {
            return;
        }
    }
}
