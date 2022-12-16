using Dotmim.Sync.Builders;
using Dotmim.Sync.SqlServer.Manager;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer.ChangeTracking.Builders
{
    public class SqlChangeTrackingBuilderTrackingTable
    {
        private ParserName tableName;
        private ParserName trackingName;
        private readonly SyncTable tableDescription;
        private readonly SyncSetup setup;
        private readonly SqlDbMetadata sqlDbMetadata;

        public SqlChangeTrackingBuilderTrackingTable(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
        {
            this.tableDescription = tableDescription;
            this.setup = setup;
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.sqlDbMetadata = new SqlDbMetadata();
        }

        public Task<DbCommand> GetExistsTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $"IF EXISTS (Select top 1 tbl.name as TableName, " +
                              $"sch.name as SchemaName " +
                              $"  from sys.change_tracking_tables tr " +
                              $"  Inner join sys.tables as tbl on tbl.object_id = tr.object_id " +
                              $"  Inner join sys.schemas as sch on tbl.schema_id = sch.schema_id " +
                              $"  Where tbl.name = @tableName and (sch.name = @schemaName or @schemaName is null)) SELECT 1 ELSE SELECT 0;";

            var tbl = tableName.Unquoted().ToString();
            var schemaNameString = string.IsNullOrEmpty(tableName.SchemaName) ? null : tableName.SchemaName;

            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = commandText;

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tableName";
            parameter.Value = tbl;
            command.Parameters.Add(parameter);

            parameter = command.CreateParameter();
            parameter.ParameterName = "@schemaName";
            parameter.Value = schemaNameString;
            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }

        public Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();

            if (setup.HasTableWithColumns(tableDescription.TableName))
            {
                command.CommandText = $"ALTER TABLE {tableName.Schema().Quoted().ToString()} ENABLE CHANGE_TRACKING WITH(TRACK_COLUMNS_UPDATED = ON);";
            }
            else
            {
                command.CommandText = $"ALTER TABLE {tableName.Schema().Quoted().ToString()} ENABLE CHANGE_TRACKING WITH(TRACK_COLUMNS_UPDATED = OFF);";
            }

            command.Connection = connection;
            command.Transaction = transaction;

            return Task.FromResult(command);
        }

        public Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $"ALTER TABLE {tableName.Schema().Quoted().ToString()} DISABLE CHANGE_TRACKING;";

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = commandText;

            return Task.FromResult(command);
        }

        public Task<DbCommand> GetRenameTrackingTableCommandAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction)
            => Task.FromResult<DbCommand>(null);
    }
}
