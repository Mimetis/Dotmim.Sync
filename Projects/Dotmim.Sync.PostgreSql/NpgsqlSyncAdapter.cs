using Dotmim.Sync.Builders;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace Dotmim.Sync.PostgreSql.Builders
{
    public class NpgsqlSyncAdapter : DbSyncAdapter
    {
        public NpgsqlDbMetadata sqlMetadata { get; private set; }
        public NpgsqlObjectNames sqlobjectNames { get; private set; }
        public NpgsqlSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName, bool useBulkOperations) : base(tableDescription, setup, scopeName, useBulkOperations)
        {
            this.sqlobjectNames = new NpgsqlObjectNames(tableDescription, tableName, trackingTableName, setup, scopeName);
            this.sqlMetadata = new NpgsqlDbMetadata();
        }
        public override (DbCommand, bool) GetCommand(DbCommandType nameType, SyncFilter filter = null)
        {
            var command = new NpgsqlCommand();
            var isBatch = false;
            switch (nameType)
            {
                case DbCommandType.SelectChanges:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.sqlobjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectChanges, filter);
                    break;
                case DbCommandType.SelectInitializedChanges:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.sqlobjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectInitializedChanges, filter);
                    break;
                case DbCommandType.SelectInitializedChangesWithFilters:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.sqlobjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectInitializedChangesWithFilters, filter);
                    break;
                case DbCommandType.SelectChangesWithFilters:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.sqlobjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectChangesWithFilters, filter);
                    break;
                case DbCommandType.SelectRow:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.sqlobjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectRow, filter);
                    break;
                case DbCommandType.UpdateRow:
                case DbCommandType.InsertRow:
                case DbCommandType.UpdateRows:
                case DbCommandType.InsertRows:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.sqlobjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.UpdateRow, filter);
                    break;
                case DbCommandType.DeleteRow:
                case DbCommandType.DeleteRows:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.sqlobjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.DeleteRow, filter);
                    break;
                case DbCommandType.DisableConstraints:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.sqlobjectNames.GetCommandName(DbCommandType.DisableConstraints, filter);
                    break;
                case DbCommandType.EnableConstraints:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.sqlobjectNames.GetCommandName(DbCommandType.EnableConstraints, filter);
                    break;
                case DbCommandType.DeleteMetadata:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.sqlobjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.DeleteMetadata, filter);
                    break;
                case DbCommandType.UpdateMetadata:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.sqlobjectNames.GetCommandName(DbCommandType.UpdateMetadata, filter);
                    break;
                case DbCommandType.SelectMetadata:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.sqlobjectNames.GetCommandName(DbCommandType.SelectMetadata, filter);
                    break;
                case DbCommandType.InsertTrigger:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.sqlobjectNames.GetTriggerCommandName(DbTriggerType.Insert, filter);
                    break;
                case DbCommandType.UpdateTrigger:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.sqlobjectNames.GetTriggerCommandName(DbTriggerType.Update, filter);
                    break;
                case DbCommandType.DeleteTrigger:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.sqlobjectNames.GetTriggerCommandName(DbTriggerType.Delete, filter);
                    break;
                case DbCommandType.UpdateUntrackedRows:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.sqlobjectNames.GetCommandName(DbCommandType.UpdateUntrackedRows, filter);
                    break;
                case DbCommandType.Reset:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.sqlobjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.Reset, filter);
                    break;
                default:
                    throw new NotImplementedException($"This command type {nameType} is not implemented");
            }

            return (command, isBatch);
        }
        public override Task AddCommandParametersAsync(DbCommandType commandType, DbCommand command, DbConnection connection, DbTransaction transaction = null, SyncFilter filter = null)
        {

            if (command == null)
                return Task.CompletedTask;

            if (command.Parameters != null && command.Parameters.Count > 0)
                return Task.CompletedTask;

            switch (commandType)
            {
                case DbCommandType.SelectChanges:
                case DbCommandType.SelectChangesWithFilters:
                case DbCommandType.SelectInitializedChanges:
                case DbCommandType.SelectInitializedChangesWithFilters:
                    this.SetSelectChangesParameters(command, filter);
                    break;
                case DbCommandType.SelectRow:
                    this.SetSelectRowParameters(command);
                    break;
                case DbCommandType.DeleteMetadata:
                    this.SetDeleteMetadataParameters(command);
                    break;
                case DbCommandType.SelectMetadata:
                    this.SetSelectMetadataParameters(command);
                    break;
                case DbCommandType.DeleteRow:
                case DbCommandType.DeleteRows:
                    this.SetDeleteRowParameters(command);
                    break;
                case DbCommandType.UpdateRow:
                case DbCommandType.InsertRow:
                case DbCommandType.UpdateRows:
                case DbCommandType.InsertRows:
                    this.SetUpdateRowParameters(command);
                    break;
                case DbCommandType.UpdateMetadata:
                    this.SetUpdateMetadataParameters(command);
                    break;
                default:
                    break;
            }

            return Task.CompletedTask;
        }
        private void SetUpdateMetadataParameters(DbCommand command)
        {
            DbParameter p;

            foreach (var column in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(column).Unquoted().Normalized().ToString();

                p = command.CreateParameter();
                p.ParameterName = $"{columnName}";
                p.DbType = column.GetDbType();
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = "sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "sync_row_is_tombstone";
            p.DbType = DbType.Boolean;
            command.Parameters.Add(p);

        }
        private void SetSelectMetadataParameters(DbCommand command)
        {
            DbParameter p;

            foreach (var column in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(column).Unquoted().Normalized().ToString();

                p = command.CreateParameter();
                p.ParameterName = $"{columnName}";
                p.DbType = column.GetDbType();
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }
        }
        private void SetUpdateRowParameters(DbCommand command)
        {
            DbParameter p;

            var prefix_parameter = NpgsqlBuilderProcedure.NPGSQL_PREFIX_PARAMETER;

            foreach (var column in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(column).Unquoted().Normalized().ToString();

                p = command.CreateParameter();
                p.ParameterName = $"{prefix_parameter}{columnName}";
                p.DbType = column.GetDbType();
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = "sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "sync_force_write";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "sync_min_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "sync_row_count";
            p.DbType = DbType.Int32;
            p.Direction = ParameterDirection.Output;
            command.Parameters.Add(p);

        }
        private void SetDeleteRowParameters(DbCommand command)
        {
            DbParameter p;
            var prefix_parameter = NpgsqlBuilderProcedure.NPGSQL_PREFIX_PARAMETER;
            foreach (var column in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var quotedColumn = ParserName.Parse(column).Unquoted().Normalized().ToString();

                p = command.CreateParameter();
                p.ParameterName = $"{prefix_parameter}{quotedColumn}";
                p.DbType = column.GetDbType();
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = "sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "sync_force_write";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "sync_min_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "sync_row_count";
            p.DbType = DbType.Int32;
            p.Direction = ParameterDirection.Output;
            command.Parameters.Add(p);
        }
        private void SetSelectRowParameters(DbCommand command)
        {
            DbParameter p;
            var prefix_parameter = NpgsqlBuilderProcedure.NPGSQL_PREFIX_PARAMETER;
            foreach (var column in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var quotedColumn = ParserName.Parse(column).Unquoted().Normalized().ToString();

                p = command.CreateParameter();
                p.ParameterName = $"{prefix_parameter}{quotedColumn}";
                p.DbType = column.GetDbType();
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = "sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

        }
        private void SetDeleteMetadataParameters(DbCommand command)
        {
            var p = command.CreateParameter();
            p.ParameterName = "sync_row_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);
        }
        private void SetSelectChangesParameters(DbCommand command, SyncFilter filter = null)
        {
            var originalProvider = NpgsqlSyncProvider.ProviderType;

            var p = command.CreateParameter();
            p.ParameterName = "sync_min_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            if (filter == null)
                return;

            var parameters = filter.Parameters;

            if (parameters.Count == 0)
                return;

            foreach (var param in parameters)
            {
                if (param.DbType.HasValue)
                {
                    // Get column name and type
                    var columnName = ParserName.Parse(param.Name).Unquoted().Normalized().ToString();
                    var syncColumn = new SyncColumn(columnName)
                    {
                        DbType = (int)param.DbType.Value,
                        MaxLength = param.MaxLength,
                    };
                    var sqlDbType = this.sqlMetadata.GetOwnerDbTypeFromDbType(syncColumn);

                    var customParameterFilter = new NpgsqlParameter($"in_{columnName}", sqlDbType)
                    {
                        Size = param.MaxLength,
                        IsNullable = param.AllowNull,
                        Value = param.DefaultValue
                    };
                    command.Parameters.Add(customParameterFilter);
                }
                else
                {
                    var tableFilter = this.TableDescription.Schema.Tables[param.TableName, param.SchemaName];
                    if (tableFilter == null)
                        throw new FilterParamTableNotExistsException(param.TableName);

                    var columnFilter = tableFilter.Columns[param.Name];
                    if (columnFilter == null)
                        throw new FilterParamColumnNotExistsException(param.Name, param.TableName);

                    // Get column name and type
                    var columnName = ParserName.Parse(columnFilter).Unquoted().Normalized().ToString();


                    var sqlDbType = tableFilter.OriginalProvider == originalProvider ?
                        this.sqlMetadata.GetNpgsqlDbType(columnFilter) : this.sqlMetadata.GetOwnerDbTypeFromDbType(columnFilter);

                    // Add it as parameter
                    var sqlParamFilter = new NpgsqlParameter($"in_{columnName}", sqlDbType)
                    {
                        Size = columnFilter.MaxLength,
                        IsNullable = param.AllowNull,
                        Value = param.DefaultValue
                    };
                    command.Parameters.Add(sqlParamFilter);
                }

            }

        }
        public override Task ExecuteBatchCommandAsync(DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> arrayItems, SyncTable schemaChangesTable, SyncTable failedRows, long? lastTimestamp, DbConnection connection, DbTransaction transaction) 
            => throw new NotImplementedException();
    }
}
