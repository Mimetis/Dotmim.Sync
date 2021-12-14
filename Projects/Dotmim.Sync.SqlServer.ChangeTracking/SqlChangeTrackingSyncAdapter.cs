using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient.Server;
using Dotmim.Sync.SqlServer.Builders;

namespace Dotmim.Sync.SqlServer
{
    public class SqlChangeTrackingSyncAdapter : SqlSyncAdapter
    {
        public SqlChangeTrackingSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup) : base(tableDescription, tableName, trackingName, setup)
        {
        }

        /// <summary>
        /// Overriding adapter since the update metadata is not a stored proc that we can override
        /// </summary>
        public override (DbCommand, bool) GetCommand(DbCommandType nameType, SyncFilter filter)
        {
            if (nameType == DbCommandType.UpdateMetadata)
            {
                var c = new SqlCommand("Set @sync_row_count = 1;");
                c.Parameters.Add("@sync_row_count", SqlDbType.Int);
                return (c, false);
            }

            return base.GetCommand(nameType, filter);
        }

    }
}
