using Dotmim.Sync.Builders;
using Dotmim.Sync.Manager;
using Dotmim.Sync.PostgreSql.Builders;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.PostgreSql.Builders
{
    public class NpgsqlTableBuilder : DbTableBuilder
    {
        public NpgsqlTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
            : base(tableDescription, tableName, trackingTableName, setup, scopeName)
        {
            this.dbMetadata = new NpgsqlDbMetadata();

            this.builderTable = new NpgsqlBuilderTable(tableDescription, tableName, trackingTableName, Setup);
            this.builderTrackingTable = new NpgsqlBuilderTrackingTable(tableDescription, tableName, trackingTableName, Setup);
            this.builderTrigger = new NpgsqlBuilderTrigger(tableDescription, tableName, trackingTableName, Setup, scopeName);
        }

        public NpgsqlBuilderTable builderTable { get; }
        public NpgsqlBuilderTrackingTable builderTrackingTable { get; }
        public NpgsqlBuilderTrigger builderTrigger { get; }
        public NpgsqlDbMetadata dbMetadata { get; }

        public override Task<DbCommand> GetAddColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
           => this.builderTable.GetAddColumnCommandAsync(columnName, connection, transaction);

        public override Task<IEnumerable<SyncColumn>> GetColumnsAsync(DbConnection connection, DbTransaction transaction)
           => this.builderTable.GetColumnsAsync(connection, transaction);

        public override Task<DbCommand> GetCreateSchemaCommandAsync(DbConnection connection, DbTransaction transaction)
                             => this.builderTable.GetCreateSchemaCommandAsync(connection, transaction);
        public override Task<DbCommand> GetCreateTableCommandAsync(DbConnection connection, DbTransaction transaction)
                    => this.builderTable.GetCreateTableCommandAsync(connection, transaction);
        public override Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.builderTrackingTable.GetCreateTrackingTableCommandAsync(connection, transaction);

        public override Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => this.builderTrigger.GetCreateTriggerCommandAsync(triggerType, connection, transaction);

        public override Task<DbCommand> GetDropColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
            => this.builderTable.GetDropColumnCommandAsync(columnName, connection, transaction);

        // ---------------------------------
        // No stored procedures
        // ---------------------------------
        public override Task<DbCommand> GetDropStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => Task.FromResult<DbCommand>(null);
        public override Task<DbCommand> GetExistsStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
           => Task.FromResult<DbCommand>(null);
        public override Task<DbCommand> GetCreateStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => Task.FromResult<DbCommand>(null);
        // ---------------------------------

        public override Task<DbCommand> GetDropTableCommandAsync(DbConnection connection, DbTransaction transaction)
           => this.builderTable.GetDropTableCommandAsync(connection, transaction);

        public override Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
           => this.builderTrackingTable.GetDropTrackingTableCommandAsync(connection, transaction);

        public override Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
             => this.builderTrigger.GetDropTriggerCommandAsync(triggerType, connection, transaction);

        public override Task<DbCommand> GetExistsColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
           => this.builderTable.GetExistsColumnCommandAsync(columnName, connection, transaction);

        public override Task<DbCommand> GetExistsSchemaCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.builderTable.GetExistsSchemaCommandAsync(connection, transaction);

        public override Task<DbCommand> GetExistsTableCommandAsync(DbConnection connection, DbTransaction transaction)
                                                                                            => this.builderTable.GetExistsTableCommandAsync(connection, transaction);
        public override Task<DbCommand> GetExistsTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.builderTrackingTable.GetExistsTrackingTableCommandAsync(connection, transaction);

        public override Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => this.builderTrigger.GetExistsTriggerCommandAsync(triggerType, connection, transaction);

        public override Task<IEnumerable<SyncColumn>> GetPrimaryKeysAsync(DbConnection connection, DbTransaction transaction)
            => this.builderTable.GetPrimaryKeysAsync(connection, transaction);

        public override Task<IEnumerable<DbRelationDefinition>> GetRelationsAsync(DbConnection connection, DbTransaction transaction)
                                    => this.builderTable.GetRelationsAsync(connection, transaction);
        public override Task<DbCommand> GetRenameTrackingTableCommandAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction)
             => this.builderTrackingTable.GetRenameTrackingTableCommandAsync(oldTableName, connection, transaction);
    }
}
