using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;
using Dotmim.Sync.Manager;
using Dotmim.Sync.SqlServer.Manager;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer.Builders
{

    /// <summary>
    /// The SqlBuilder class is the Sql implementation of DbBuilder class.
    /// In charge of creating tracking table, stored proc, triggers and adapters.
    /// </summary>
    public class SqlTableBuilder : DbTableBuilder
    {
        /// <summary>
        /// Gets the SqlObjectNames.
        /// </summary>
        public SqlObjectNames SqlObjectNames { get; }

        /// <summary>
        /// Gets the SqlDbMetadata.
        /// </summary>
        public SqlDbMetadata SqlDbMetadata { get; }

        private SqlBuilderProcedure sqlBuilderProcedure;
        private SqlBuilderTable sqlBuilderTable;
        private SqlBuilderTrackingTable sqlBuilderTrackingTable;
        private SqlBuilderTrigger sqlBuilderTrigger;

        private DbTableNames tableNames;
        private DbTableNames trackingTableNames;

        /// <inheritdoc cref="SqlTableBuilder" />
        public SqlTableBuilder(SyncTable tableDescription, ScopeInfo scopeInfo)
            : base(tableDescription, scopeInfo)
        {
            this.SqlObjectNames = new SqlObjectNames(tableDescription, scopeInfo);

            this.tableNames = new DbTableNames(
                SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote,
                this.SqlObjectNames.TableName,
                this.SqlObjectNames.TableNormalizedFullName,
                this.SqlObjectNames.TableNormalizedShortName,
                this.SqlObjectNames.TableQuotedFullName,
                this.SqlObjectNames.TableQuotedShortName,
                this.SqlObjectNames.TableSchemaName);

            this.trackingTableNames = new DbTableNames(
                SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote,
                this.SqlObjectNames.TrackingTableName,
                this.SqlObjectNames.TrackingTableNormalizedFullName,
                this.SqlObjectNames.TrackingTableNormalizedShortName,
                this.SqlObjectNames.TrackingTableQuotedFullName,
                this.SqlObjectNames.TrackingTableQuotedShortName,
                this.SqlObjectNames.TrackingTableSchemaName);

            this.SqlDbMetadata = new SqlDbMetadata();

            this.sqlBuilderProcedure = new SqlBuilderProcedure(tableDescription, this.SqlObjectNames, this.SqlDbMetadata);
            this.sqlBuilderTable = new SqlBuilderTable(tableDescription, this.SqlObjectNames, this.SqlDbMetadata);
            this.sqlBuilderTrackingTable = new SqlBuilderTrackingTable(tableDescription, this.SqlObjectNames, this.SqlDbMetadata);
            this.sqlBuilderTrigger = new SqlBuilderTrigger(tableDescription, this.SqlObjectNames, this.SqlDbMetadata);
        }

        /// <inheritdoc/>
        public override DbColumnNames GetParsedColumnNames(SyncColumn column)
        {
            var columnParser = new ObjectParser(column.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
            return new(columnParser.QuotedShortName, columnParser.NormalizedShortName);
        }

        /// <inheritdoc/>
        public override DbTableNames GetParsedTableNames() => this.tableNames;

        /// <inheritdoc/>
        public override DbTableNames GetParsedTrackingTableNames() => this.trackingTableNames;

        /// <inheritdoc/>
        public override Task<DbCommand> GetCreateSchemaCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetCreateSchemaCommandAsync(connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetCreateTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetCreateTableCommandAsync(connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetExistsTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetExistsTableCommandAsync(connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetExistsSchemaCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetExistsSchemaCommandAsync(connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetDropTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetDropTableCommandAsync(connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetExistsColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetExistsColumnCommandAsync(columnName, connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetAddColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetAddColumnCommandAsync(columnName, connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetDropColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetDropColumnCommandAsync(columnName, connection, transaction);

        /// <inheritdoc/>
        public override Task<IEnumerable<SyncColumn>> GetColumnsAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetColumnsAsync(connection, transaction);

        /// <inheritdoc/>
        public override Task<IEnumerable<DbRelationDefinition>> GetRelationsAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetRelationsAsync(connection, transaction);

        /// <inheritdoc/>
        public override Task<IEnumerable<SyncColumn>> GetPrimaryKeysAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetPrimaryKeysAsync(connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetExistsStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderProcedure.GetExistsStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetCreateStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderProcedure.GetCreateStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetDropStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderProcedure.GetDropStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrackingTable.GetCreateTrackingTableCommandAsync(connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrackingTable.GetDropTrackingTableCommandAsync(connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetExistsTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrackingTable.GetExistsTrackingTableCommandAsync(connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrigger.GetExistsTriggerCommandAsync(triggerType, connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrigger.GetCreateTriggerCommandAsync(triggerType, connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrigger.GetDropTriggerCommandAsync(triggerType, connection, transaction);
    }
}