using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;
using Dotmim.Sync.Manager;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

#if MARIADB
namespace Dotmim.Sync.MariaDB.Builders
#elif MYSQL
namespace Dotmim.Sync.MySql.Builders
#endif
{

    /// <summary>
    /// The MySqlBuilder class is the MySql implementation of DbBuilder class.
    /// In charge of creating tracking table, stored proc, triggers and adapters.
    /// </summary>
    public class MySqlTableBuilder : DbTableBuilder
    {
        /// <summary>
        /// Gets the SqlObjectNames.
        /// </summary>
        public MySqlObjectNames MySqlObjectNames { get; }

        private readonly DbTableNames tableNames;
        private readonly DbTableNames trackingTableNames;

        /// <summary>
        /// Gets the SqlDbMetadata.
        /// </summary>
        public MySqlDbMetadata MySqlDbMetadata { get; }

        private MySqlBuilderProcedure sqlBuilderProcedure;
        private MySqlBuilderTable sqlBuilderTable;
        private MySqlBuilderTrackingTable sqlBuilderTrackingTable;
        private MySqlBuilderTrigger sqlBuilderTrigger;

        /// <inheritdoc cref="MySqlTableBuilder" />
        public MySqlTableBuilder(SyncTable tableDescription, ScopeInfo scopeInfo)
            : base(tableDescription, scopeInfo)
        {
            this.MySqlObjectNames = new MySqlObjectNames(tableDescription, scopeInfo);

            this.tableNames = new DbTableNames(
                MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote,
                this.MySqlObjectNames.TableName,
                this.MySqlObjectNames.TableNormalizedFullName,
                this.MySqlObjectNames.TableNormalizedShortName,
                this.MySqlObjectNames.TableQuotedFullName,
                this.MySqlObjectNames.TableQuotedShortName,
                this.MySqlObjectNames.TableSchemaName);

            this.trackingTableNames = new DbTableNames(
                MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote,
                this.MySqlObjectNames.TrackingTableName,
                this.MySqlObjectNames.TrackingTableNormalizedShortName,
                this.MySqlObjectNames.TrackingTableNormalizedFullName,
                this.MySqlObjectNames.TrackingTableQuotedShortName,
                this.MySqlObjectNames.TrackingTableQuotedFullName,
                this.MySqlObjectNames.TrackingTableSchemaName);

            this.MySqlDbMetadata = new MySqlDbMetadata();

            this.sqlBuilderProcedure = new MySqlBuilderProcedure(tableDescription, this.MySqlObjectNames, this.MySqlDbMetadata, scopeInfo);
            this.sqlBuilderTable = new MySqlBuilderTable(tableDescription, this.MySqlObjectNames, this.MySqlDbMetadata);
            this.sqlBuilderTrackingTable = new MySqlBuilderTrackingTable(tableDescription, this.MySqlObjectNames, this.MySqlDbMetadata);
            this.sqlBuilderTrigger = new MySqlBuilderTrigger(tableDescription, this.MySqlObjectNames, this.MySqlDbMetadata);
        }

        /// <inheritdoc />
        public override Task<DbCommand> GetCreateSchemaCommandAsync(DbConnection connection, DbTransaction transaction)
            => null;

        /// <inheritdoc />
        public override Task<DbCommand> GetCreateTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetCreateTableCommandAsync(connection, transaction);

        /// <inheritdoc />
        public override Task<DbCommand> GetExistsTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetExistsTableCommandAsync(connection, transaction);

        /// <inheritdoc />
        public override Task<DbCommand> GetExistsSchemaCommandAsync(DbConnection connection, DbTransaction transaction)
            => null;

        /// <inheritdoc />
        public override Task<DbCommand> GetDropTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetDropTableCommandAsync(connection, transaction);

        /// <inheritdoc />
        public override Task<IEnumerable<SyncColumn>> GetColumnsAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetColumnsAsync(connection, transaction);

        /// <inheritdoc />
        public override Task<IEnumerable<DbRelationDefinition>> GetRelationsAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetRelationsAsync(connection, transaction);

        /// <inheritdoc />
        public override Task<IEnumerable<SyncColumn>> GetPrimaryKeysAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetPrimaryKeysAsync(connection, transaction);

        /// <inheritdoc />
        public override Task<DbCommand> GetExistsStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderProcedure.GetExistsStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction);

        /// <inheritdoc />
        public override Task<DbCommand> GetCreateStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderProcedure.GetCreateStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction);

        /// <inheritdoc />
        public override Task<DbCommand> GetDropStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderProcedure.GetDropStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction);

        /// <inheritdoc />
        public override Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrackingTable.GetCreateTrackingTableCommandAsync(connection, transaction);

        /// <inheritdoc />
        public override Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrackingTable.GetDropTrackingTableCommandAsync(connection, transaction);

        /// <inheritdoc />
        public override Task<DbCommand> GetExistsTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrackingTable.GetExistsTrackingTableCommandAsync(connection, transaction);

        /// <inheritdoc />
        public override Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrigger.GetExistsTriggerCommandAsync(triggerType, connection, transaction);

        /// <inheritdoc />
        public override Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrigger.GetCreateTriggerCommandAsync(triggerType, connection, transaction);

        /// <inheritdoc />
        public override Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrigger.GetDropTriggerCommandAsync(triggerType, connection, transaction);

        /// <inheritdoc />
        public override Task<DbCommand> GetExistsColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetExistsColumnCommandAsync(columnName, connection, transaction);

        /// <inheritdoc />
        public override Task<DbCommand> GetAddColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetAddColumnCommandAsync(columnName, connection, transaction);

        /// <inheritdoc />
        public override Task<DbCommand> GetDropColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetDropColumnCommandAsync(columnName, connection, transaction);

        /// <inheritdoc />
        public override DbTableNames GetParsedTableNames() => this.tableNames;

        /// <inheritdoc />
        public override DbTableNames GetParsedTrackingTableNames() => this.trackingTableNames;

        /// <inheritdoc />
        public override DbColumnNames GetParsedColumnNames(SyncColumn column)
        {
            var columnParser = new ObjectParser(column.ColumnName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);
            return new(columnParser.QuotedShortName, columnParser.NormalizedShortName);
        }
    }
}