//using Dotmim.Sync.Manager;
//using Dotmim.Sync.SqlServer.Manager;
//using Dotmim.Sync.SqlServer.Scope;
//using Microsoft.Data.SqlClient;
//using Newtonsoft.Json;
//using System;
//using System.Collections.Generic;
//using System.Data;
//using System.Data.Common;
//using System.Diagnostics;
//using System.Text;
//using System.Threading.Tasks;

//namespace Dotmim.Sync.SqlServer.ChangeTracking.Builders
//{
//    public class SqlChangeTrackingScopeInfoBuilder : SqlScopeInfoBuilder
//    {
//        public SqlChangeTrackingScopeInfoBuilder(string scopeTableName) : base(scopeTableName)
//        {
//        }

//        public override async Task<long> GetLocalTimestampAsync(DbConnection connection, DbTransaction transaction)
//        {
//                var commandText = "SELECT @sync_new_timestamp = CHANGE_TRACKING_CURRENT_VERSION();";

//            using (var command = new SqlCommand(commandText, (SqlConnection)connection, (SqlTransaction)transaction))
//            {
//                DbParameter p = command.CreateParameter();
//                p.ParameterName = "@sync_new_timestamp";
//                p.DbType = DbType.Int64;
//                p.Direction = ParameterDirection.Output;
//                command.Parameters.Add(p);

//                await command.ExecuteNonQueryAsync().ConfigureAwait(false);

//                var outputParameter = DbSyncAdapter.GetParameter(command, "sync_new_timestamp");

//                if (outputParameter == null)
//                    return 0L;

//                long.TryParse(outputParameter.Value.ToString(), out long result);

//                return Math.Max(result, 0);
//            }
//        }

      
//    }
//}
