using System;
using System.Collections.Generic;
using System.Linq;

using System.Data.Common;
using System.Data;
using Dotmim.Sync.Builders;
#if NET6_0 || NET8_0 
using MySqlConnector;
using System.Reflection.Metadata;
#elif NETSTANDARD
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

    public class MySqlSyncAdapter : DbSyncAdapter
    {
        public MySqlObjectNames MySqlObjectNames { get; }
        public MySqlDbMetadata MySqlDbMetadata { get; }

        public MySqlSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup, string scopeName) : base(tableDescription, setup, scopeName)
        {
            this.MySqlDbMetadata = new MySqlDbMetadata();
            this.MySqlObjectNames = new MySqlObjectNames(TableDescription, tableName, trackingName, Setup, scopeName);

        }
        public override DbCommand EnsureCommandParameters(DbCommand command, DbCommandType commandType, DbConnection connection, DbTransaction transaction, SyncFilter filter = null)
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
                        _ => parameter.ParameterName
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
                        _ => parameter.ParameterName
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
                        p.ParameterName = p.ParameterName.Replace("in_", "");

                    if (!p.ParameterName.StartsWith("@"))
                        p.ParameterName = $"@{p.ParameterName}";
                }
            }

            return command;
        }

        public override DbCommand EnsureCommandParametersValues(DbCommand command, DbCommandType commandType, DbConnection connection, DbTransaction transaction)
        {
            return command;
        }

        public override string ParameterPrefix => "in_";
        public override string QuotePrefix => "`";
        public override string QuoteSuffix => "`";
        public override bool SupportsOutputParameters => true;

        public override (DbCommand, bool) GetCommand(DbCommandType nameType, SyncFilter filter = null)
        {
            var command = new MySqlCommand();
            var isBatch = false;
            switch (nameType)
            {
                case DbCommandType.SelectChanges:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.MySqlObjectNames.CreateSelectIncrementalChangesCommand(filter);
                    break;
                case DbCommandType.SelectInitializedChanges:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.MySqlObjectNames.CreateSelectInitializedChangesCommand(filter);
                    break;
                case DbCommandType.SelectInitializedChangesWithFilters:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.MySqlObjectNames.CreateSelectInitializedChangesCommand(filter);
                    break;
                case DbCommandType.SelectChangesWithFilters:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.MySqlObjectNames.CreateSelectIncrementalChangesCommand(filter);
                    break;
                case DbCommandType.SelectRow:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.MySqlObjectNames.CreateSelectRowCommand();
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
                    command.CommandText = this.MySqlObjectNames.GetCommandName(DbCommandType.DisableConstraints, filter);
                    break;
                case DbCommandType.EnableConstraints:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.MySqlObjectNames.GetCommandName(DbCommandType.EnableConstraints, filter);
                    break;
                case DbCommandType.DeleteMetadata:
                    //command.CommandType = CommandType.StoredProcedure;
                    //command.CommandText = this.MySqlObjectNames.GetCommandName(DbCommandType.DeleteMetadata, filter);
                    //break;
                    return (default, false);
                case DbCommandType.UpdateMetadata:
                    //command.CommandType = CommandType.Text;
                    //command.CommandText = this.MySqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata, filter);
                    //break;
                    return (default, false);
                case DbCommandType.SelectMetadata:
                    //command.CommandType = CommandType.Text;
                    //command.CommandText = this.MySqlObjectNames.GetCommandName(DbCommandType.SelectMetadata, filter);
                    //break;
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
                    command.CommandText = this.MySqlObjectNames.CreateUpdateUntrackedRowsCommand();
                    break;
                case DbCommandType.Reset:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.MySqlObjectNames.CreateResetCommand();
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
                    throw new NotImplementedException($"This command type {nameType} is not implemented");
            }

            return (command, isBatch);
        }


        public override Task ExecuteBatchCommandAsync(DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> arrayItems, SyncTable schemaChangesTable, SyncTable failedRows, long? lastTimestamp, DbConnection connection, DbTransaction transaction = null)
            => throw new NotImplementedException();
    }
}
