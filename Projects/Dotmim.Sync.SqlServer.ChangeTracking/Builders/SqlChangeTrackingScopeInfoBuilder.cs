using Dotmim.Sync.SqlServer.Manager;
using Dotmim.Sync.SqlServer.Scope;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer.ChangeTracking.Builders
{
    public class SqlChangeTrackingScopeInfoBuilder : SqlScopeInfoBuilder
    {
        public SqlChangeTrackingScopeInfoBuilder(string scopeTableName, DbConnection connection, DbTransaction transaction = null) 
            : base(scopeTableName, connection, transaction)
        {
        }

        public override async Task<long> GetLocalTimestampAsync()
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;

            bool alreadyOpened = connection.State == ConnectionState.Open;
            try
            {
                command.CommandText = "SELECT @sync_new_timestamp = CHANGE_TRACKING_CURRENT_VERSION();";
                DbParameter p = command.CreateParameter();
                p.ParameterName = "@sync_new_timestamp";
                p.DbType = DbType.Int64;
                p.Direction = ParameterDirection.Output;
                command.Parameters.Add(p);

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                var outputParameter = SqlManager.GetParameter(command, "sync_new_timestamp");

                if (outputParameter == null)
                    return 0L;

                long result = 0L;

                long.TryParse(outputParameter.Value.ToString(), out result);

                command.Dispose();

                return Math.Max(result, 0);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during GetLocalTimestamp : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }
    }
}
