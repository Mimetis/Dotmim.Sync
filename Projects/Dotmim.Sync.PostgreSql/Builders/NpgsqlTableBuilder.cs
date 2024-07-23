using Dotmim.Sync.Builders;
using Dotmim.Sync.Manager;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync.PostgreSql.Builders
{
    public class NpgsqlTableBuilder : DbTableBuilder
    {
        public NpgsqlTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
            : base(tableDescription, tableName, trackingTableName, setup, scopeName)
        {
            this.DbMetadata = new NpgsqlDbMetadata();

            this.BuilderTable = new NpgsqlBuilderTable(tableDescription, tableName, trackingTableName, this.Setup);
            this.BuilderTrackingTable = new NpgsqlBuilderTrackingTable(tableDescription, tableName, trackingTableName, this.Setup);
            this.BuilderTrigger = new NpgsqlBuilderTrigger(tableDescription, tableName, trackingTableName, this.Setup, scopeName);
        }

        public NpgsqlBuilderTable BuilderTable { get; }

        public NpgsqlBuilderTrackingTable BuilderTrackingTable { get; }

        public NpgsqlBuilderTrigger BuilderTrigger { get; }

        public NpgsqlDbMetadata DbMetadata { get; }

        public override Task<DbCommand> GetAddColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
           => this.BuilderTable.GetAddColumnCommandAsync(columnName, connection, transaction);

        public override Task<IEnumerable<SyncColumn>> GetColumnsAsync(DbConnection connection, DbTransaction transaction)
           => this.BuilderTable.GetColumnsAsync(connection, transaction);

        public override Task<DbCommand> GetCreateSchemaCommandAsync(DbConnection connection, DbTransaction transaction)
                             => this.BuilderTable.GetCreateSchemaCommandAsync(connection, transaction);

        public override Task<DbCommand> GetCreateTableCommandAsync(DbConnection connection, DbTransaction transaction)
                    => this.BuilderTable.GetCreateTableCommandAsync(connection, transaction);

        public override Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.BuilderTrackingTable.GetCreateTrackingTableCommandAsync(connection, transaction);

        public override Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => this.BuilderTrigger.GetCreateTriggerCommandAsync(triggerType, connection, transaction);

        public override Task<DbCommand> GetDropColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
            => this.BuilderTable.GetDropColumnCommandAsync(columnName, connection, transaction);

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
           => this.BuilderTable.GetDropTableCommandAsync(connection, transaction);

        public override Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
           => this.BuilderTrackingTable.GetDropTrackingTableCommandAsync(connection, transaction);

        public override Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
             => this.BuilderTrigger.GetDropTriggerCommandAsync(triggerType, connection, transaction);

        public override Task<DbCommand> GetExistsColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
           => this.BuilderTable.GetExistsColumnCommandAsync(columnName, connection, transaction);

        public override Task<DbCommand> GetExistsSchemaCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.BuilderTable.GetExistsSchemaCommandAsync(connection, transaction);

        public override Task<DbCommand> GetExistsTableCommandAsync(DbConnection connection, DbTransaction transaction)
                                                                                            => this.BuilderTable.GetExistsTableCommandAsync(connection, transaction);

        public override Task<DbCommand> GetExistsTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.BuilderTrackingTable.GetExistsTrackingTableCommandAsync(connection, transaction);

        public override Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => this.BuilderTrigger.GetExistsTriggerCommandAsync(triggerType, connection, transaction);

        public override Task<IEnumerable<SyncColumn>> GetPrimaryKeysAsync(DbConnection connection, DbTransaction transaction)
            => this.BuilderTable.GetPrimaryKeysAsync(connection, transaction);

        public override Task<IEnumerable<DbRelationDefinition>> GetRelationsAsync(DbConnection connection, DbTransaction transaction)
                                    => this.BuilderTable.GetRelationsAsync(connection, transaction);

        [Obsolete("DMS is not renaming tracking tables anymore")]
        public override Task<DbCommand> GetRenameTrackingTableCommandAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction)
             => this.BuilderTrackingTable.GetRenameTrackingTableCommandAsync(oldTableName, connection, transaction);
    }
}