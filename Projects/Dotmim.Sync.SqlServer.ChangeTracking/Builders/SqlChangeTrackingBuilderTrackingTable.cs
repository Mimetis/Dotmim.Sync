using Dotmim.Sync.Builders;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer.Builders
{

    /// <summary>
    /// Sql Change Tracking Builder Tracking Table.
    /// </summary>
    public class SqlChangeTrackingBuilderTrackingTable
    {
        private readonly SqlObjectNames sqlObjectNames;

        /// <inheritdoc cref="SqlChangeTrackingBuilderTrackingTable"/>
        public SqlChangeTrackingBuilderTrackingTable(SqlObjectNames sqlObjectNames) => this.sqlObjectNames = sqlObjectNames;

        /// <summary>
        /// Get the command to check if the tracking table exists.
        /// </summary>
        public Task<DbCommand> GetExistsTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $"IF EXISTS (Select top 1 tbl.name as TableName, " +
                              $"sch.name as SchemaName " +
                              $"  from sys.change_tracking_tables tr " +
                              $"  Inner join sys.tables as tbl on tbl.object_id = tr.object_id " +
                              $"  Inner join sys.schemas as sch on tbl.schema_id = sch.schema_id " +
                              $"  Where tbl.name = @tableName and sch.name = @schemaName) SELECT 1 ELSE SELECT 0;";

            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = commandText;

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tableName";
            parameter.Value = this.sqlObjectNames.TableName;
            command.Parameters.Add(parameter);

            parameter = command.CreateParameter();
            parameter.ParameterName = "@schemaName";
            parameter.Value = this.sqlObjectNames.TableSchemaName;
            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }

        /// <summary>
        /// Get the command to create the tracking table.
        /// </summary>
        public Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();

            command.CommandText = $"ALTER TABLE {this.sqlObjectNames.TableQuotedFullName} ENABLE CHANGE_TRACKING;";
            command.Connection = connection;
            command.Transaction = transaction;

            return Task.FromResult(command);
        }

        /// <summary>
        /// Get the command to drop the tracking table.
        /// </summary>
        public Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $"ALTER TABLE {this.sqlObjectNames.TableQuotedFullName} DISABLE CHANGE_TRACKING;";

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = commandText;

            return Task.FromResult(command);
        }

        /// <summary>
        /// Get the command to rename the tracking table. (Not used anymore).
        /// </summary>
        public Task<DbCommand> GetRenameTrackingTableCommandAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction)
            => Task.FromResult<DbCommand>(null);
    }
}