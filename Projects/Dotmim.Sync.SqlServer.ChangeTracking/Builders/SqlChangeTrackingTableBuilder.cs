using Dotmim.Sync.Builders;
using Dotmim.Sync.SqlServer.Builders;
using Dotmim.Sync.SqlServer.Manager;
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

        public new SqlObjectNames SqlObjectNames { get; }

        public new SqlDbMetadata SqlDbMetadata { get; }

        public SqlChangeTrackingTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
            : base(tableDescription, tableName, trackingTableName, setup, scopeName)
        {
            this.SqlObjectNames = new SqlObjectNames(tableDescription, setup, scopeName);
            this.SqlDbMetadata = new SqlDbMetadata();

            this.sqlChangeTrackingBuilderTrackingTable = new SqlChangeTrackingBuilderTrackingTable(this.TableDescription, this.TableName, this.TrackingTableName, this.Setup);
            this.sqlChangeTrackingBuilderProcedure = new SqlChangeTrackingBuilderProcedure(this.TableDescription, this.Setup, this.SqlObjectNames, this.SqlDbMetadata);
            this.sqlChangeTrackingBuilderTrigger = new SqlChangeTrackingBuilderTrigger(this.TableDescription, this.Setup, this.SqlObjectNames, this.SqlDbMetadata);
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