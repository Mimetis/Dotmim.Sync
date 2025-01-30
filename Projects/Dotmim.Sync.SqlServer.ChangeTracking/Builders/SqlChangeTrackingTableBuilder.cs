using Dotmim.Sync.Builders;
using Dotmim.Sync.SqlServer.Builders;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer.ChangeTracking.Builders
{
    /// <inheritdoc />
    public class SqlChangeTrackingTableBuilder : SqlTableBuilder
    {
        private readonly SqlChangeTrackingBuilderTrackingTable sqlChangeTrackingBuilderTrackingTable;
        private readonly SqlChangeTrackingBuilderProcedure sqlChangeTrackingBuilderProcedure;
        private readonly SqlChangeTrackingBuilderTrigger sqlChangeTrackingBuilderTrigger;

        /// <inheritdoc />
        public SqlChangeTrackingTableBuilder(SyncTable tableDescription, ScopeInfo scopeInfo)
            : base(tableDescription, scopeInfo)
        {

            this.sqlChangeTrackingBuilderTrackingTable = new SqlChangeTrackingBuilderTrackingTable(this.SqlObjectNames);
            this.sqlChangeTrackingBuilderProcedure = new SqlChangeTrackingBuilderProcedure(this.TableDescription, this.SqlObjectNames, this.SqlDbMetadata);
            this.sqlChangeTrackingBuilderTrigger = new SqlChangeTrackingBuilderTrigger(this.TableDescription, this.SqlObjectNames, this.SqlDbMetadata);
        }

        /// <inheritdoc />
        public override Task<DbCommand> GetExistsStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => this.sqlChangeTrackingBuilderProcedure.GetExistsStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction);

        /// <inheritdoc />
        public override Task<DbCommand> GetCreateStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => this.sqlChangeTrackingBuilderProcedure.GetCreateStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction);

        /// <inheritdoc />
        public override Task<DbCommand> GetDropStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => this.sqlChangeTrackingBuilderProcedure.GetDropStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction);

        /// <inheritdoc />
        public override Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlChangeTrackingBuilderTrackingTable.GetCreateTrackingTableCommandAsync(connection, transaction);

        /// <inheritdoc />
        public override Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlChangeTrackingBuilderTrackingTable.GetDropTrackingTableCommandAsync(connection, transaction);

        /// <inheritdoc />
        public override Task<DbCommand> GetExistsTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlChangeTrackingBuilderTrackingTable.GetExistsTrackingTableCommandAsync(connection, transaction);

        /// <inheritdoc />
        public override Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => this.sqlChangeTrackingBuilderTrigger.GetExistsTriggerCommandAsync(triggerType, connection, transaction);

        /// <inheritdoc />
        public override Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => this.sqlChangeTrackingBuilderTrigger.GetCreateTriggerCommandAsync(triggerType, connection, transaction);

        /// <inheritdoc />
        public override Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => this.sqlChangeTrackingBuilderTrigger.GetDropTriggerCommandAsync(triggerType, connection, transaction);
    }
}