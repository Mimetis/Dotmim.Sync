using Dotmim.Sync.Builders;
using Dotmim.Sync.Manager;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer.Builders
{

    /// <summary>
    /// The SqlBuilder class is the Sql implementation of DbBuilder class.
    /// In charge of creating tracking table, stored proc, triggers and adapters.
    /// </summary>
    public class SqlTableBuilder : DbTableBuilder
    {
        private SqlBuilderProcedure sqlBuilderProcedure;
        private SqlBuilderTable sqlBuilderTable;
        private SqlBuilderTrackingTable sqlBuilderTrackingTable;
        private SqlBuilderTrigger sqlBuilderTrigger;

        public SqlTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup) : base(tableDescription, tableName, trackingTableName, setup)
        {
            this.sqlBuilderProcedure = new SqlBuilderProcedure(tableDescription, tableName, trackingTableName, Setup);
            this.sqlBuilderTable = new SqlBuilderTable(tableDescription, tableName, trackingTableName, Setup);
            this.sqlBuilderTrackingTable = new SqlBuilderTrackingTable(tableDescription, tableName, trackingTableName, Setup);
            this.sqlBuilderTrigger = new SqlBuilderTrigger(tableDescription, tableName, trackingTableName, Setup);
        }

        public override Task<DbCommand> GetCreateSchemaCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetCreateSchemaCommandAsync(connection, transaction);
        public override Task<DbCommand> GetCreateTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetCreateTableCommandAsync(connection, transaction);
        public override Task<DbCommand> GetExistsTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetExistsTableCommandAsync(connection, transaction);
        public override Task<DbCommand> GetExistsSchemaCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetExistsSchemaCommandAsync(connection, transaction);
        public override Task<DbCommand> GetDropTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetDropTableCommandAsync(connection, transaction);

        public override Task<IEnumerable<SyncColumn>> GetColumnsAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetColumnsAsync(connection, transaction);
        public override Task<IEnumerable<DbRelationDefinition>> GetRelationsAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetRelationsAsync(connection, transaction);
        public override Task<IEnumerable<SyncColumn>> GetPrimaryKeysAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetPrimaryKeysAsync(connection, transaction);


        public override Task<DbCommand> GetExistsStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderProcedure.GetExistsStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction);
        public override Task<DbCommand> GetCreateStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderProcedure.GetCreateStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction);
        public override Task<DbCommand> GetDropStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderProcedure.GetDropStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction);

        public override Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrackingTable.GetCreateTrackingTableCommandAsync(connection, transaction);
        public override Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrackingTable.GetDropTrackingTableCommandAsync(connection, transaction);
        public override Task<DbCommand> GetRenameTrackingTableCommandAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrackingTable.GetRenameTrackingTableCommandAsync(oldTableName, connection, transaction);
        public override Task<DbCommand> GetExistsTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrackingTable.GetExistsTrackingTableCommandAsync(connection, transaction);

        public override Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrigger.GetExistsTriggerCommandAsync(triggerType, connection, transaction);
        public override Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrigger.GetCreateTriggerCommandAsync(triggerType, connection, transaction);
        public override Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrigger.GetDropTriggerCommandAsync(triggerType, connection, transaction);
    }
}
