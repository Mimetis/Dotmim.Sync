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
using MySql.Data.MySqlClient;
using Dotmim.Sync.DatabaseStringParsers;

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

    public partial class MySqlSyncAdapter : DbSyncAdapter
    {
        public MySqlObjectNames MySqlObjectNames { get; }

        public MySqlDbMetadata MySqlDbMetadata { get; }

        public MySqlSyncAdapter(SyncTable tableDescription, ScopeInfo scopeInfo)
            : base(tableDescription, scopeInfo)
        {
            this.MySqlDbMetadata = new MySqlDbMetadata();
            this.MySqlObjectNames = new MySqlObjectNames(this.TableDescription, scopeInfo);
        }

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

                    if (!p.ParameterName.StartsWith("@"))
                        p.ParameterName = $"@{p.ParameterName}";
                }
            }

            return command;
        }

        public override DbCommand EnsureCommandParametersValues(SyncContext context, DbCommand command, DbCommandType commandType, DbConnection connection, DbTransaction transaction)
        {
            return command;
        }

        public override string ParameterPrefix => "in_";

        public override bool SupportsOutputParameters => true;

        public override (DbCommand, bool) GetCommand(SyncContext context, DbCommandType commandType, SyncFilter filter = null)
        {
            var command = new MySqlCommand();
            var isBatch = false;
            switch (commandType)
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
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.MySqlObjectNames.GetCommandName(DbCommandType.DeleteMetadata, filter);
                    break;
                case DbCommandType.UpdateMetadata:
                    // command.CommandType = CommandType.Text;
                    // command.CommandText = this.MySqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata, filter);
                    // break;
                    return (default, false);
                case DbCommandType.SelectMetadata:
                    // command.CommandType = CommandType.Text;
                    // command.CommandText = this.MySqlObjectNames.GetCommandName(DbCommandType.SelectMetadata, filter);
                    // break;
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
                    throw new NotImplementedException($"This command type {commandType} is not implemented");
            }

            return (command, isBatch);
        }

        public override Task ExecuteBatchCommandAsync(SyncContext context, DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> arrayItems, SyncTable schemaChangesTable, SyncTable failedRows, long? lastTimestamp, DbConnection connection, DbTransaction transaction = null)
            => throw new NotImplementedException();

        public override DbColumnNames GetParsedColumnNames(string name) => throw new NotImplementedException();

        public override DbTableBuilder GetTableBuilder() => throw new NotImplementedException();

        internal MySqlParameter GetMySqlParameter(SyncColumn column)
        {
#if MARIADB
            var originalProvider = MariaDBSyncProvider.ProviderType;
#elif MYSQL
            var originalProvider = MySqlSyncProvider.ProviderType;
#endif
            var columParser = new ObjectParser(column.ColumnName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);
            var parameterName = ParserName.Parse(column, "`").Unquoted().Normalized().ToString();

            // Get the good SqlDbType (even if we are not from Sql Server def)
            var mySqlDbType = this.TableDescription.OriginalProvider == originalProvider ?
                this.MySqlDbMetadata.GetMySqlDbType(column) : this.MySqlDbMetadata.GetOwnerDbTypeFromDbType(column);

            var sqlParameter = new MySqlParameter
            {
                ParameterName = $"{MYSQLPREFIXPARAMETER}{parameterName}",
                DbType = column.GetDbType(),
                IsNullable = column.AllowDBNull,
                MySqlDbType = mySqlDbType,
                SourceColumn = string.IsNullOrEmpty(column.ExtraProperty1) ? null : column.ExtraProperty1,
            };

            (byte precision, byte scale) = this.MySqlDbMetadata.GetCompatibleColumnPrecisionAndScale(column, this.TableDescription.OriginalProvider);

            if ((sqlParameter.DbType == DbType.Decimal || sqlParameter.DbType == DbType.Double
                 || sqlParameter.DbType == DbType.Single || sqlParameter.DbType == DbType.VarNumeric) && precision > 0)
            {
                sqlParameter.Precision = precision;
                if (scale > 0)
                    sqlParameter.Scale = scale;
            }
            else
            {
                sqlParameter.Size = column.MaxLength > 0 ? column.MaxLength : sqlParameter.DbType == DbType.Guid ? 36 : -1;
            }

            return sqlParameter;
        }
    }
}