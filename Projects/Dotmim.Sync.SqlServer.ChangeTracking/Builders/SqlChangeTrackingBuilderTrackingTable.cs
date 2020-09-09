using Dotmim.Sync.Builders;



using Dotmim.Sync.SqlServer.Builders;
using Dotmim.Sync.SqlServer.Manager;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer.ChangeTracking.Builders
{
    public class SqlChangeTrackingBuilderTrackingTable : IDbBuilderTrackingTableHelper
    {
        private ParserName tableName;
        private ParserName trackingName;
        private readonly SyncTable tableDescription;
        private readonly SyncSetup setup;
        private readonly SqlDbMetadata sqlDbMetadata;

        public SyncFilter Filter { get; set; }

        public SqlChangeTrackingBuilderTrackingTable(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
        {
            this.tableDescription = tableDescription;
            this.setup = setup;
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.sqlDbMetadata = new SqlDbMetadata();
        }

        public async Task CreateTableAsync(DbConnection connection, DbTransaction transaction)
        {

            var changetrackingtable = await SqlChangeTrackingManagementUtils.ChangeTrackingTableAsync(
                (SqlConnection)connection, (SqlTransaction)transaction, tableName.ToString(), tableName.SchemaName);

            if (changetrackingtable !=  null && changetrackingtable.Rows != null && changetrackingtable.Rows.Count > 0)
                return;

            var commandText = $"ALTER TABLE {tableName.Schema().Quoted().ToString()} ENABLE CHANGE_TRACKING WITH(TRACK_COLUMNS_UPDATED = OFF);";

            using var command = new SqlCommand(commandText, (SqlConnection)connection, (SqlTransaction)transaction);
            
            await command.ExecuteNonQueryAsync().ConfigureAwait(false);

        }

        public async Task DropTableAsync(DbConnection connection, DbTransaction transaction)
        {
            var changetrackingtable = await SqlChangeTrackingManagementUtils.ChangeTrackingTableAsync(
                (SqlConnection)connection, (SqlTransaction)transaction, tableName.ToString(), tableName.SchemaName);

            if (changetrackingtable == null || changetrackingtable.Rows == null || changetrackingtable.Rows.Count <= 0)
                return;

            var commandText = $"ALTER TABLE {tableName.Schema().Quoted().ToString()} DISABLE CHANGE_TRACKING;";

            using var command = new SqlCommand(commandText, (SqlConnection)connection, (SqlTransaction)transaction);

            await command.ExecuteNonQueryAsync().ConfigureAwait(false);

        }


        public Task RenameTableAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction) => Task.CompletedTask;
    }
}
