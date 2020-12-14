using Dotmim.Sync.Builders;



using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlChangeTrackingBuilderTrigger : SqlBuilderTrigger
    {
        public SqlChangeTrackingBuilderTrigger(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
            : base(tableDescription, tableName, trackingName, setup)
        {
        }

        public override Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction) => null;
        public override Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction) => null;
        public override Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction) => null;
    }


}
