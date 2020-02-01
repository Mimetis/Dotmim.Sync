using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Text;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlBuilder : DbBuilder
    {
        public override void EnsureDatabase(DbConnection connection, DbTransaction transaction = null)
        {
            // Chek if db exists
            if (!SqlManagementUtils.DatabaseExists(connection as SqlConnection, transaction as SqlTransaction))
                throw new MissingDatabaseException(connection.Database);

            // Check if we are using change tracking and it's enabled on the source
            if (this.UseChangeTracking && !SqlManagementUtils.IsChangeTrackingEnabled(connection as SqlConnection, transaction as SqlTransaction))
                throw new MissingChangeTrackingException(connection.Database);

        }
    }
}
