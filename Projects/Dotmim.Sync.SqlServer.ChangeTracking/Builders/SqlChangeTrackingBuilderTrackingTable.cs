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
            var commandText = this.CreateTableCommandText();

            using (var command = new SqlCommand(commandText, (SqlConnection)connection, (SqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }

        }

        public async Task DropTableAsync(DbConnection connection, DbTransaction transaction)
        {
            using (var command = new SqlCommand(this.CreateDropTableCommandText(), (SqlConnection)connection, (SqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }


        private string CreateDropTableCommandText() => $"ALTER TABLE {tableName.Schema().Quoted().ToString()} DISABLE CHANGE_TRACKING;";

        private string CreateTableCommandText() => $"ALTER TABLE {tableName.Schema().Quoted().ToString()} ENABLE CHANGE_TRACKING WITH(TRACK_COLUMNS_UPDATED = OFF);";

        public async Task<bool> NeedToCreateTrackingTableAsync(DbConnection connection, DbTransaction transaction)
        {
            var schemaName = this.tableName.SchemaName;
            var tableName = this.tableName.ObjectName;

            var table = await SqlChangeTrackingManagementUtils.ChangeTrackingTableAsync((SqlConnection)connection, (SqlTransaction)transaction, tableName, schemaName);

            return table.Rows.Count <= 0;
        }


        public Task CreateIndexAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;
        public Task CreatePkAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;
        public Task RenameTableAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction) => Task.CompletedTask;
    }
}
