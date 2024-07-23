using Dotmim.Sync.Builders;
using Dotmim.Sync.SqlServer.Builders;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer.ChangeTracking.Builders
{
    public class SqlChangeTrackingTableBuilder : SqlTableBuilder
    {
        private SqlChangeTrackingBuilderTrackingTable sqlChangeTrackingBuilderTrackingTable;
        private SqlChangeTrackingBuilderProcedure sqlChangeTrackingBuilderProcedure;
        private SqlChangeTrackingBuilderTrigger sqlChangeTrackingBuilderTrigger;

        public SqlChangeTrackingTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
            : base(tableDescription, tableName, trackingTableName, setup, scopeName)
        {
            this.sqlChangeTrackingBuilderTrackingTable = new SqlChangeTrackingBuilderTrackingTable(this.TableDescription, this.TableName, this.TrackingTableName, this.Setup);
            this.sqlChangeTrackingBuilderProcedure = new SqlChangeTrackingBuilderProcedure(this.TableDescription, this.TableName, this.TrackingTableName, this.Setup, scopeName);
            this.sqlChangeTrackingBuilderTrigger = new SqlChangeTrackingBuilderTrigger(this.TableDescription, this.TableName, this.TrackingTableName, this.Setup, scopeName);
        }

        public override Task<DbCommand> GetExistsStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => this.sqlChangeTrackingBuilderProcedure.GetExistsStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction);

        public override Task<DbCommand> GetCreateStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => this.sqlChangeTrackingBuilderProcedure.GetCreateStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction);

        public override Task<DbCommand> GetDropStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => this.sqlChangeTrackingBuilderProcedure.GetDropStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction);

        public override Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlChangeTrackingBuilderTrackingTable.GetCreateTrackingTableCommandAsync(connection, transaction);

        public override Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlChangeTrackingBuilderTrackingTable.GetDropTrackingTableCommandAsync(connection, transaction);

        [Obsolete("DMS is not renaming tracking table anymore")]
        public override Task<DbCommand> GetRenameTrackingTableCommandAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction)
            => this.sqlChangeTrackingBuilderTrackingTable.GetRenameTrackingTableCommandAsync(oldTableName, connection, transaction);

        public override Task<DbCommand> GetExistsTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlChangeTrackingBuilderTrackingTable.GetExistsTrackingTableCommandAsync(connection, transaction);

        public override Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => this.sqlChangeTrackingBuilderTrigger.GetExistsTriggerCommandAsync(triggerType, connection, transaction);

        public override Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => this.sqlChangeTrackingBuilderTrigger.GetCreateTriggerCommandAsync(triggerType, connection, transaction);

        public override Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => this.sqlChangeTrackingBuilderTrigger.GetDropTriggerCommandAsync(triggerType, connection, transaction);
    }
}