using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;
using Dotmim.Sync.Manager;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync.PostgreSql.Builders
{
    /// <summary>
    /// Represents a table builder for Npgsql.
    /// </summary>
    public class NpgsqlTableBuilder : DbTableBuilder
    {
        private DbTableNames tableNames;
        private DbTableNames trackingTableNames;

        /// <summary>
        /// Gets the ,npgsql object names.
        /// </summary>
        protected NpgsqlObjectNames NpgsqlObjectNames { get; }

        /// <summary>
        /// Gets the npgsql database metadata.
        /// </summary>
        protected NpgsqlDbMetadata NpgsqlDbMetadata { get; }

        /// <summary>
        /// Gets the table builder.
        /// </summary>
        public NpgsqlBuilderTable BuilderTable { get; }

        /// <summary>
        /// Gets the tracking table builder.
        /// </summary>
        public NpgsqlBuilderTrackingTable BuilderTrackingTable { get; }

        /// <summary>
        /// Gets the trigger builder.
        /// </summary>
        public NpgsqlBuilderTrigger BuilderTrigger { get; }

        /// <inheritdoc cref="NpgsqlTableBuilder"/>
        public NpgsqlTableBuilder(SyncTable tableDescription, ScopeInfo scopeInfo)
            : base(tableDescription, scopeInfo)
        {

            this.NpgsqlDbMetadata = new NpgsqlDbMetadata();
            this.NpgsqlObjectNames = new NpgsqlObjectNames(tableDescription, scopeInfo);

            this.tableNames = new DbTableNames(
                  NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote,
                  this.NpgsqlObjectNames.TableName,
                  this.NpgsqlObjectNames.TableNormalizedFullName,
                  this.NpgsqlObjectNames.TableNormalizedShortName,
                  this.NpgsqlObjectNames.TableQuotedFullName,
                  this.NpgsqlObjectNames.TableQuotedShortName,
                  this.NpgsqlObjectNames.TableSchemaName);

            this.trackingTableNames = new DbTableNames(
                NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote,
                this.NpgsqlObjectNames.TrackingTableName,
                this.NpgsqlObjectNames.TrackingTableNormalizedShortName,
                this.NpgsqlObjectNames.TrackingTableNormalizedFullName,
                this.NpgsqlObjectNames.TrackingTableQuotedShortName,
                this.NpgsqlObjectNames.TrackingTableQuotedFullName,
                this.NpgsqlObjectNames.TrackingTableSchemaName);

            this.BuilderTable = new NpgsqlBuilderTable(tableDescription, this.NpgsqlObjectNames, this.NpgsqlDbMetadata);
            this.BuilderTrackingTable = new NpgsqlBuilderTrackingTable(tableDescription, this.NpgsqlObjectNames, this.NpgsqlDbMetadata);
            this.BuilderTrigger = new NpgsqlBuilderTrigger(tableDescription, this.NpgsqlObjectNames, this.NpgsqlDbMetadata);
        }

        /// <inheritdoc/>
        public override DbColumnNames GetParsedColumnNames(SyncColumn column)
        {
            var columnParser = new ObjectParser(column.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
            return new(columnParser.QuotedShortName, columnParser.NormalizedShortName);
        }

        /// <inheritdoc/>
        public override DbTableNames GetParsedTableNames() => this.tableNames;

        /// <inheritdoc/>
        public override DbTableNames GetParsedTrackingTableNames() => this.trackingTableNames;

        /// <inheritdoc/>
        public override Task<DbCommand> GetAddColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
           => this.BuilderTable.GetAddColumnCommandAsync(columnName, connection, transaction);

        /// <inheritdoc/>
        public override Task<IEnumerable<SyncColumn>> GetColumnsAsync(DbConnection connection, DbTransaction transaction)
           => this.BuilderTable.GetColumnsAsync(connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetCreateSchemaCommandAsync(DbConnection connection, DbTransaction transaction)
                             => this.BuilderTable.GetCreateSchemaCommandAsync(connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetCreateTableCommandAsync(DbConnection connection, DbTransaction transaction)
                    => this.BuilderTable.GetCreateTableCommandAsync(connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.BuilderTrackingTable.GetCreateTrackingTableCommandAsync(connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => this.BuilderTrigger.GetCreateTriggerCommandAsync(triggerType, connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetDropColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
            => this.BuilderTable.GetDropColumnCommandAsync(columnName, connection, transaction);

        /// <summary>
        /// Returns null as Npgsql does not use persisted stored procedures.
        /// </summary>
        public override Task<DbCommand> GetDropStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => Task.FromResult<DbCommand>(null);

        /// <summary>
        /// Returns null as Npgsql does not use persisted stored procedures.
        /// </summary>
        public override Task<DbCommand> GetExistsStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
           => Task.FromResult<DbCommand>(null);

        /// <summary>
        /// Returns null as Npgsql does not use persisted stored procedures.
        /// </summary>
        public override Task<DbCommand> GetCreateStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => Task.FromResult<DbCommand>(null);

        /// <inheritdoc/>
        public override Task<DbCommand> GetDropTableCommandAsync(DbConnection connection, DbTransaction transaction)
           => this.BuilderTable.GetDropTableCommandAsync(connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
           => this.BuilderTrackingTable.GetDropTrackingTableCommandAsync(connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
             => this.BuilderTrigger.GetDropTriggerCommandAsync(triggerType, connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetExistsColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
           => this.BuilderTable.GetExistsColumnCommandAsync(columnName, connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetExistsSchemaCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.BuilderTable.GetExistsSchemaCommandAsync(connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetExistsTableCommandAsync(DbConnection connection, DbTransaction transaction)
                                                                                            => this.BuilderTable.GetExistsTableCommandAsync(connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetExistsTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.BuilderTrackingTable.GetExistsTrackingTableCommandAsync(connection, transaction);

        /// <inheritdoc/>
        public override Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => this.BuilderTrigger.GetExistsTriggerCommandAsync(triggerType, connection, transaction);

        /// <inheritdoc/>
        public override Task<IEnumerable<SyncColumn>> GetPrimaryKeysAsync(DbConnection connection, DbTransaction transaction)
            => this.BuilderTable.GetPrimaryKeysAsync(connection, transaction);

        /// <inheritdoc/>
        public override Task<IEnumerable<DbRelationDefinition>> GetRelationsAsync(DbConnection connection, DbTransaction transaction)
                                    => this.BuilderTable.GetRelationsAsync(connection, transaction);
    }
}