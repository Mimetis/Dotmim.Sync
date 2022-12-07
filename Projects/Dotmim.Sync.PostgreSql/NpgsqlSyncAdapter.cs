using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.PostgreSql.Builders;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using static Npgsql.Replication.PgOutput.Messages.RelationMessage;

namespace Dotmim.Sync.PostgreSql.Builders
{
    public class NpgsqlSyncAdapter : DbSyncAdapter
    {
        public static DateTime SqlDateMin = new DateTime(1753, 1, 1);
        public static DateTime SqlSmallDateMin = new DateTime(1900, 1, 1);
        private static ConcurrentDictionary<string, List<NpgsqlParameter>> derivingParameters
             = new ConcurrentDictionary<string, List<NpgsqlParameter>>();

        private string scopeName;
        private SyncSetup setup;
        private SyncTable tableDescription;
        private ParserName tableName;
        private ParserName trackingTableName;
        public NpgsqlSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName, bool useBulkOperations) : base(tableDescription, setup, scopeName, useBulkOperations)
        {
            this.tableDescription = tableDescription;
            this.tableName = tableName;
            this.trackingTableName = trackingTableName;
            this.setup = setup;
            this.scopeName = scopeName;
            this.UseBulkOperations = useBulkOperations;
            this.NpgsqlObjectNames = new NpgsqlObjectNames(tableDescription, tableName, trackingTableName, setup, scopeName);
            this.SqlMetadata = new NpgsqlDbMetadata();
        }

        public NpgsqlObjectNames NpgsqlObjectNames { get; set; }
        public NpgsqlDbMetadata SqlMetadata { get; set; }
        public override Task AddCommandParametersAsync(DbCommandType commandType, DbCommand command, DbConnection connection, DbTransaction transaction, SyncFilter filter = null)
        {
            if (command == null)
                return Task.CompletedTask;

            if (command.Parameters != null && command.Parameters.Count > 0)
                return Task.CompletedTask;
            switch (commandType)
            {
                case DbCommandType.DisableConstraints:
                case DbCommandType.EnableConstraints:
                    break;
                case DbCommandType.UpdateMetadata:
                    this.SetUpdateRowParameters(command);
                    break;
                case DbCommandType.SelectMetadata:
                    this.SetSelectRowParameters(command);
                    break;
                case DbCommandType.SelectChanges:
                case DbCommandType.SelectChangesWithFilters:
                case DbCommandType.SelectInitializedChanges:
                case DbCommandType.SelectInitializedChangesWithFilters:
                    this.SetSelectChangesParameters(command, commandType, filter);
                    break;
                default:
                    break;
            }
            return Task.CompletedTask;
        }

        public override Task ExecuteBatchCommandAsync(DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> arrayItems, SyncTable schemaChangesTable, SyncTable failedRows, long? lastTimestamp, DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
        public override (DbCommand Command, bool IsBatchCommand) GetCommand(DbCommandType nameType, SyncFilter filter = null)
        {
            var command = new NpgsqlCommand();
            bool isBatch = false;
            switch (nameType)
            {
                case DbCommandType.SelectChanges:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectChanges, filter);
                    isBatch = false;
                    break;
                case DbCommandType.SelectInitializedChanges:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectInitializedChanges, filter);
                    isBatch = false;
                    break;
                case DbCommandType.SelectInitializedChangesWithFilters:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectInitializedChangesWithFilters, filter);
                    isBatch = false;
                    break;
                case DbCommandType.SelectChangesWithFilters:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectChangesWithFilters, filter);
                    isBatch = false;
                    break;
                case DbCommandType.SelectRow:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectRow, filter);
                    isBatch = false;
                    break;
                case DbCommandType.UpdateRow:
                case DbCommandType.InsertRow:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.UpdateRow, filter);
                    isBatch = false;
                    break;
                case DbCommandType.UpdateRows:
                case DbCommandType.InsertRows:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.UpdateRow, filter);
                    //if (this.UseBulkOperations)
                    //{
                    //    command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkUpdateRows, filter);
                    //    isBatch = true;
                    //}
                    //else
                    //{
                    //    command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.UpdateRow, filter);
                    //    isBatch = false;
                    //}
                    break;
                case DbCommandType.DeleteRow:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.DeleteRow, filter);
                    isBatch = false;
                    break;
                case DbCommandType.DeleteRows:
                    command.CommandType = CommandType.StoredProcedure;
                    if (this.UseBulkOperations)
                    {
                        command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkDeleteRows, filter);
                        isBatch = true;
                    }
                    else
                    {
                        command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.DeleteRow, filter);
                        isBatch = false;
                    }
                    break;
                case DbCommandType.DisableConstraints:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.NpgsqlObjectNames.GetCommandName(DbCommandType.DisableConstraints, filter);
                    isBatch = false;
                    break;
                case DbCommandType.EnableConstraints:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.NpgsqlObjectNames.GetCommandName(DbCommandType.EnableConstraints, filter);
                    isBatch = false;
                    break;
                case DbCommandType.DeleteMetadata:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.DeleteMetadata, filter);
                    isBatch = false;
                    break;
                case DbCommandType.UpdateMetadata:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.NpgsqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata, filter);
                    isBatch = false;
                    break;
                case DbCommandType.SelectMetadata:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.NpgsqlObjectNames.GetCommandName(DbCommandType.SelectMetadata, filter);
                    isBatch = false;
                    break;
                case DbCommandType.InsertTrigger:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.NpgsqlObjectNames.GetTriggerCommandName(DbTriggerType.Insert, filter);
                    isBatch = false;
                    break;
                case DbCommandType.UpdateTrigger:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.NpgsqlObjectNames.GetTriggerCommandName(DbTriggerType.Update, filter);
                    isBatch = false;
                    break;
                case DbCommandType.DeleteTrigger:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.NpgsqlObjectNames.GetTriggerCommandName(DbTriggerType.Delete, filter);
                    isBatch = false;
                    break;
                case DbCommandType.BulkTableType:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkTableType, filter);
                    isBatch = false;
                    break;
                case DbCommandType.UpdateUntrackedRows:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.NpgsqlObjectNames.GetCommandName(DbCommandType.UpdateUntrackedRows, filter);
                    isBatch = false;
                    break;
                case DbCommandType.Reset:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.Reset, filter);
                    isBatch = false;
                    break;

                default:
                    throw new NotImplementedException($"This command type {nameType} is not implemented");
            }

            return (command, isBatch);
        }
        private void SetSelectChangesParameters(DbCommand command, DbCommandType commandType, SyncFilter filter)
        {
            var originalProvider = NpgsqlSyncProvider.ProviderType;

            var p = command.CreateParameter();
            p.ParameterName = "sync_min_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            if (commandType == DbCommandType.SelectChanges || commandType == DbCommandType.SelectChangesWithFilters)
            {
                p = command.CreateParameter();
                p.ParameterName = "sync_scope_id";
                p.DbType = DbType.Guid;
                command.Parameters.Add(p);
            }

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
                    var sqlDbType = this.SqlMetadata.GetOwnerDbTypeFromDbType(syncColumn);

                    var customParameterFilter = new NpgsqlParameter($"{columnName}", sqlDbType);
                    customParameterFilter.Size = param.MaxLength;
                    customParameterFilter.IsNullable = param.AllowNull;
                    customParameterFilter.Value = param.DefaultValue;

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
                    var columnName = ParserName.Parse(columnFilter).Normalized().Unquoted().ToString();

                    var sqlDbType = tableFilter.OriginalProvider == originalProvider ?
                        this.SqlMetadata.GetNpgsqlDbType(columnFilter) : this.SqlMetadata.GetOwnerDbTypeFromDbType(columnFilter);

                    // Add it as parameter
                    var sqlParamFilter = new NpgsqlParameter($"{columnName}", sqlDbType);
                    sqlParamFilter.Size = columnFilter.MaxLength;
                    sqlParamFilter.IsNullable = param.AllowNull;
                    sqlParamFilter.Value = param.DefaultValue;
                    command.Parameters.Add(sqlParamFilter);
                }
            }
        }
        private void SetSelectRowParameters(DbCommand command)
        {
            DbParameter p;

            foreach (var column in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var unquotedColumn = ParserName.Parse(column).Normalized().Unquoted().ToString();
                p = command.CreateParameter();
                p.ParameterName = $"{unquotedColumn}";
                p.DbType = column.GetDbType();
                p.SourceColumn = column.ColumnName;
                p.Size = column.MaxLength;
                command.Parameters.Add(p);
            }
        }
        private void SetUpdateRowParameters(DbCommand command)
        {
            DbParameter p;

            foreach (var column in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var unquotedColumn = ParserName.Parse(column).Normalized().Unquoted().ToString();
                p = command.CreateParameter();
                p.ParameterName = $"{unquotedColumn}";
                p.DbType = column.GetDbType();
                p.SourceColumn = column.ColumnName;
                p.Size = column.MaxLength;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = "sync_scope_id";
            p.DbType = DbType.Guid;
            p.Size = 32;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "sync_row_is_tombstone";
            p.DbType = DbType.Boolean;
            p.Size = 2;
            command.Parameters.Add(p);
        }

    }
}
