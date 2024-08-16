using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
#if NET6_0 || NET8_0
using Dotmim.Sync.DatabaseStringParsers;
using MySqlConnector;
using System.Reflection.Metadata;

#elif NETSTANDARD
using Dotmim.Sync.DatabaseStringParsers;
using MySql.Data.MySqlClient;

#endif
#if MARIADB
using Dotmim.Sync.MariaDB.Builders;
#elif MYSQL
using Dotmim.Sync.MySql.Builders;
#endif
using System.Threading.Tasks;

#if MARIADB
namespace Dotmim.Sync.MariaDB
#elif MYSQL
namespace Dotmim.Sync.MySql
#endif
{

    /// <summary>
    /// Represents a MySql Sync Adapter.
    /// </summary>
    public partial class MySqlSyncAdapter : DbSyncAdapter
    {
        /// <summary>
        /// Gets the MySqlObjectNames.
        /// </summary>
        public MySqlObjectNames MySqlObjectNames { get; }

        /// <summary>
        /// Gets the MySqlDbMetadata.
        /// </summary>
        public MySqlDbMetadata MySqlDbMetadata { get; }

        /// <summary>
        /// Gets the MySqlPrefixParameter.
        /// </summary>
        public override string ParameterPrefix => "in_";

        /// <inheritdoc />
        public override bool SupportsOutputParameters => true;

        /// <inheritdoc cref="MySqlSyncAdapter" />
        public MySqlSyncAdapter(SyncTable tableDescription, ScopeInfo scopeInfo)
            : base(tableDescription, scopeInfo)
        {
            this.MySqlDbMetadata = new MySqlDbMetadata();
            this.MySqlObjectNames = new MySqlObjectNames(this.TableDescription, scopeInfo);
        }

        /// <inheritdoc />
        public override DbCommand EnsureCommandParameters(SyncContext context, DbCommand command, DbCommandType commandType, DbConnection connection, DbTransaction transaction, SyncFilter filter = null)
        {
            if (commandType == DbCommandType.UpdateRow || commandType == DbCommandType.UpdateRows ||
                commandType == DbCommandType.InsertRow || commandType == DbCommandType.InsertRows ||
                commandType == DbCommandType.DeleteRow || commandType == DbCommandType.DeleteRows)
            {
                foreach (DbParameter parameter in command.Parameters)
                {
                    parameter.ParameterName = parameter.ParameterName switch
                    {
                        "in_sync_scope_id" => "sync_scope_id",
                        "in_sync_min_timestamp" => "sync_min_timestamp",
                        "in_sync_row_is_tombstone" => "sync_row_is_tombstone",
                        "in_sync_row_count" => "sync_row_count",
                        "in_sync_row_timestamp" => "sync_row_timestamp",
                        "in_sync_force_write" => "sync_force_write",
                        _ => parameter.ParameterName,
                    };
                }
            }
            else
            {
                foreach (DbParameter parameter in command.Parameters)
                {
                    parameter.ParameterName = parameter.ParameterName switch
                    {
                        "in_sync_scope_id" => "@sync_scope_id",
                        "in_sync_min_timestamp" => "@sync_min_timestamp",
                        "in_sync_row_is_tombstone" => "@sync_row_is_tombstone",
                        "in_sync_row_count" => "@sync_row_count",
                        "in_sync_row_timestamp" => "@sync_row_timestamp",
                        "in_sync_force_write" => "@sync_force_write",
                        _ => parameter.ParameterName,
                    };
                }
            }

            // for stored procedures, parameters are prefixed with "in_"
            // for command parameters are prefixed with "@" ....
            if (commandType != DbCommandType.UpdateRow && commandType != DbCommandType.UpdateRows &&
                commandType != DbCommandType.InsertRow && commandType != DbCommandType.InsertRows &&
                commandType != DbCommandType.DeleteRow && commandType != DbCommandType.DeleteRows)
            {
                foreach (var parameter in command.Parameters)
                {
                    var p = parameter as DbParameter;
                    if (p.ParameterName.StartsWith("in_"))
                        p.ParameterName = p.ParameterName.Replace("in_", string.Empty);

#if NET6_0_OR_GREATER
                    if (!p.ParameterName.StartsWith('@'))
#else
                    if (!p.ParameterName.StartsWith("@"))
#endif
                        p.ParameterName = $"@{p.ParameterName}";
                }
            }

            return command;
        }

        /// <inheritdoc />
        public override DbCommand EnsureCommandParametersValues(SyncContext context, DbCommand command, DbCommandType commandType, DbConnection connection, DbTransaction transaction)
            => command;

        /// <inheritdoc />
        public override (DbCommand, bool) GetCommand(SyncContext context, DbCommandType commandType, SyncFilter filter = null)
        {
#pragma warning disable CA2000 // Dispose objects before losing scope
            var command = new MySqlCommand();
#pragma warning restore CA2000 // Dispose objects before losing scope
            var isBatch = false;
            switch (commandType)
            {
                case DbCommandType.SelectChanges:
                case DbCommandType.SelectChangesWithFilters:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.CreateSelectIncrementalChangesCommand(filter);
                    break;
                case DbCommandType.SelectInitializedChanges:
                case DbCommandType.SelectInitializedChangesWithFilters:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.CreateSelectInitializedChangesCommand(filter);
                    break;
                case DbCommandType.SelectRow:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.CreateSelectRowCommand();
                    break;
                case DbCommandType.UpdateRow:
                case DbCommandType.InsertRow:
                case DbCommandType.UpdateRows:
                case DbCommandType.InsertRows:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.MySqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.UpdateRow, filter);
                    break;
                case DbCommandType.DeleteRow:
                case DbCommandType.DeleteRows:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.MySqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.DeleteRow, filter);
                    break;
                case DbCommandType.DisableConstraints:
                    command.CommandType = CommandType.Text;
                    command.CommandText = MySqlObjectNames.DisableConstraintsText;
                    break;
                case DbCommandType.EnableConstraints:
                    command.CommandType = CommandType.Text;
                    command.CommandText = MySqlObjectNames.EnableConstraintsText;
                    break;
                case DbCommandType.DeleteMetadata:
                    command.CommandType = CommandType.Text;
                    command.CommandText = string.Format(MySqlObjectNames.DeleteMetadataText, this.MySqlObjectNames.TrackingTableQuotedFullName);
                    break;
                case DbCommandType.UpdateMetadata:
                    return (default, false);
                case DbCommandType.SelectMetadata:
                    return (default, false);
                case DbCommandType.InsertTrigger:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.MySqlObjectNames.GetTriggerCommandName(DbTriggerType.Insert, filter);
                    break;
                case DbCommandType.UpdateTrigger:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.MySqlObjectNames.GetTriggerCommandName(DbTriggerType.Update, filter);
                    break;
                case DbCommandType.DeleteTrigger:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.MySqlObjectNames.GetTriggerCommandName(DbTriggerType.Delete, filter);
                    break;
                case DbCommandType.UpdateUntrackedRows:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.CreateUpdateUntrackedRowsCommand();
                    break;
                case DbCommandType.Reset:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.CreateResetCommand();
                    break;
                case DbCommandType.BulkTableType:
                case DbCommandType.PreUpdateRows:
                case DbCommandType.PreInsertRows:
                case DbCommandType.PreDeleteRows:
                case DbCommandType.PreUpdateRow:
                case DbCommandType.PreInsertRow:
                case DbCommandType.PreDeleteRow:
                    return (default, false);
                default:
                    throw new NotImplementedException($"This command type {commandType} is not implemented");
            }

            return (command, isBatch);
        }

        /// <summary>
        /// Not supported by MySQL.
        /// </summary>
        public override Task ExecuteBatchCommandAsync(SyncContext context, DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> arrayItems, SyncTable schemaChangesTable, SyncTable failedRows, long? lastTimestamp, DbConnection connection, DbTransaction transaction = null)
            => throw new NotImplementedException();

        /// <inheritdoc />
        public override DbColumnNames GetParsedColumnNames(string name) => throw new NotImplementedException();

        /// <inheritdoc />
        public override DbTableBuilder GetTableBuilder() => new MySqlTableBuilder(this.TableDescription, this.ScopeInfo);
    }
}