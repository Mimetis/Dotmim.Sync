using System;
using System.Collections.Generic;
using System.Linq;

using System.Data.Common;
using System.Data;
using Dotmim.Sync.Builders;
#if NET5_0 || NET6_0 || NET7_0 || NETCOREAPP3_1
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

        public MySqlObjectNames MySqlObjectNames { get; set; }
        public MySqlDbMetadata MySqlDbMetadata { get; set; }

        public MySqlSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup, string scopeName) : base(tableDescription, setup, scopeName)
        {
            this.MySqlDbMetadata = new MySqlDbMetadata();
            this.MySqlObjectNames = new MySqlObjectNames(TableDescription, tableName, trackingName, Setup, scopeName);

        }
        public override DbCommand EnsureCommandParameters(DbCommand command, DbCommandType commandType, DbConnection connection, DbTransaction transaction, SyncFilter filter = null)
        {
            // Remove in_ for all sync parameters... (historical)
            foreach(DbParameter parameter in command.Parameters)
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
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.MySqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectChanges, filter);
                    break;
                case DbCommandType.SelectInitializedChanges:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.MySqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectInitializedChanges, filter);
                    break;
                case DbCommandType.SelectInitializedChangesWithFilters:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.MySqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectInitializedChangesWithFilters, filter);
                    break;
                case DbCommandType.SelectChangesWithFilters:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.MySqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectChangesWithFilters, filter);
                    break;
                case DbCommandType.SelectRow:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.MySqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectRow, filter);
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
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.MySqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.DeleteMetadata, filter);
                    break;
                case DbCommandType.UpdateMetadata:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.MySqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata, filter);
                    break;
                case DbCommandType.SelectMetadata:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.MySqlObjectNames.GetCommandName(DbCommandType.SelectMetadata, filter);
                    break;
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
                    command.CommandText = this.MySqlObjectNames.GetCommandName(DbCommandType.UpdateUntrackedRows, filter);
                    break;
                case DbCommandType.Reset:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.MySqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.Reset, filter);
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

 
//        public override Task AddCommandParametersAsync(DbCommandType commandType, DbCommand command, DbConnection connection, DbTransaction transaction = null, SyncFilter filter = null)
//        {

//            if (command == null)
//                return Task.CompletedTask;

//            if (command.Parameters != null && command.Parameters.Count > 0)
//                return Task.CompletedTask;

//            switch (commandType)
//            {
//                case DbCommandType.SelectChanges:
//                case DbCommandType.SelectChangesWithFilters:
//                case DbCommandType.SelectInitializedChanges:
//                case DbCommandType.SelectInitializedChangesWithFilters:
//                    this.SetSelectChangesParameters(command, filter);
//                    break;
//                case DbCommandType.SelectRow:
//                    this.SetSelectRowParameters(command);
//                    break;
//                case DbCommandType.DeleteMetadata:
//                    this.SetDeleteMetadataParameters(command);
//                    break;
//                case DbCommandType.SelectMetadata:
//                    this.SetSelectMetadataParameters(command);
//                    break;
//                case DbCommandType.DeleteRow:
//                case DbCommandType.DeleteRows:
//                    this.SetDeleteRowParameters(command);
//                    break;
//                case DbCommandType.UpdateRow:
//                case DbCommandType.InsertRow:
//                case DbCommandType.UpdateRows:
//                case DbCommandType.InsertRows:
//                    this.SetUpdateRowParameters(command);
//                    break;
//                case DbCommandType.UpdateMetadata:
//                    this.SetUpdateMetadataParameters(command);
//                    break;
//                default:
//                    break;
//            }

//            return Task.CompletedTask;
//        }

//        private void SetUpdateMetadataParameters(DbCommand command)
//        {
//            DbParameter p;

//            foreach (var column in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
//            {
//                var columnName = ParserName.Parse(column, "`").Unquoted().Normalized().ToString();

//                p = command.CreateParameter();
//                p.ParameterName = $"@{columnName}";
//                p.DbType = column.GetDbType();
//                p.SourceColumn = column.ColumnName;
//                command.Parameters.Add(p);
//            }

//            p = command.CreateParameter();
//            p.ParameterName = "@sync_scope_id";
//            p.DbType = DbType.Guid;
//            command.Parameters.Add(p);

//            p = command.CreateParameter();
//            p.ParameterName = "@sync_row_is_tombstone";
//            p.DbType = DbType.Boolean;
//            command.Parameters.Add(p);

//        }

//        private void SetSelectMetadataParameters(DbCommand command)
//        {
//            DbParameter p;

//            foreach (var column in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
//            {
//                var columnName = ParserName.Parse(column, "`").Unquoted().Normalized().ToString();

//                p = command.CreateParameter();
//                p.ParameterName = $"@{columnName}";
//                p.DbType = column.GetDbType();
//                p.SourceColumn = column.ColumnName;
//                command.Parameters.Add(p);
//            }
//        }

//        private void SetUpdateRowParameters(DbCommand command)
//        {
//            DbParameter p;

//            var prefix_parameter = MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER;

//            foreach (var column in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
//            {
//                var columnName = ParserName.Parse(column, "`").Unquoted().Normalized().ToString();

//                p = command.CreateParameter();
//                p.ParameterName = $"{prefix_parameter}{columnName}";
//                p.DbType = column.GetDbType();
//                p.SourceColumn = column.ColumnName;
//                command.Parameters.Add(p);
//            }

//            p = command.CreateParameter();
//            p.ParameterName = "sync_scope_id";
//            p.DbType = DbType.Guid;
//            command.Parameters.Add(p);

//            p = command.CreateParameter();
//            p.ParameterName = "sync_force_write";
//            p.DbType = DbType.Int64;
//            command.Parameters.Add(p);

//            p = command.CreateParameter();
//            p.ParameterName = "sync_min_timestamp";
//            p.DbType = DbType.Int64;
//            command.Parameters.Add(p);

//            p = command.CreateParameter();
//            p.ParameterName = "sync_row_count";
//            p.DbType = DbType.Int32;
//            p.Direction = ParameterDirection.Output;
//            command.Parameters.Add(p);

//        }

//        private void SetDeleteRowParameters(DbCommand command)
//        {
//            DbParameter p;
//            var prefix_parameter = MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER;
//            foreach (var column in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
//            {
//                var quotedColumn = ParserName.Parse(column, "`").Unquoted().Normalized().ToString();

//                p = command.CreateParameter();
//                p.ParameterName = $"{prefix_parameter}{quotedColumn}";
//                p.DbType = column.GetDbType();
//                p.SourceColumn = column.ColumnName;
//                command.Parameters.Add(p);
//            }

//            p = command.CreateParameter();
//            p.ParameterName = "sync_scope_id";
//            p.DbType = DbType.Guid;
//            command.Parameters.Add(p);

//            p = command.CreateParameter();
//            p.ParameterName = "sync_force_write";
//            p.DbType = DbType.Int64;
//            command.Parameters.Add(p);

//            p = command.CreateParameter();
//            p.ParameterName = "sync_min_timestamp";
//            p.DbType = DbType.Int64;
//            command.Parameters.Add(p);

//            p = command.CreateParameter();
//            p.ParameterName = "sync_row_count";
//            p.DbType = DbType.Int32;
//            p.Direction = ParameterDirection.Output;
//            command.Parameters.Add(p);
//        }

//        private void SetSelectRowParameters(DbCommand command)
//        {
//            DbParameter p;
//            var prefix_parameter = MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER;
//            foreach (var column in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
//            {
//                var quotedColumn = ParserName.Parse(column, "`").Unquoted().Normalized().ToString();

//                p = command.CreateParameter();
//                p.ParameterName = $"{prefix_parameter}{quotedColumn}";
//                p.DbType = column.GetDbType();
//                p.SourceColumn = column.ColumnName;
//                command.Parameters.Add(p);
//            }

//            p = command.CreateParameter();
//            p.ParameterName = "sync_scope_id";
//            p.DbType = DbType.Guid;
//            command.Parameters.Add(p);

//        }

//        private void SetDeleteMetadataParameters(DbCommand command)
//        {
//            var p = command.CreateParameter();
//            p.ParameterName = "sync_row_timestamp";
//            p.DbType = DbType.Int64;
//            command.Parameters.Add(p);
//        }

//        private void SetSelectChangesParameters(DbCommand command, SyncFilter filter = null)
//        {
//#if MARIADB
//            var originalProvider = MariaDBSyncProvider.ProviderType;
//#elif MYSQL
//            var originalProvider = MySqlSyncProvider.ProviderType;
//#endif

//            var p = command.CreateParameter();
//            p.ParameterName = "sync_min_timestamp";
//            p.DbType = DbType.Int64;
//            command.Parameters.Add(p);

//            p = command.CreateParameter();
//            p.ParameterName = "sync_scope_id";
//            p.DbType = DbType.Guid;
//            command.Parameters.Add(p);

//            if (filter == null)
//                return;

//            var parameters = filter.Parameters;

//            if (parameters.Count == 0)
//                return;

//            foreach (var param in parameters)
//            {
//                if (param.DbType.HasValue)
//                {
//                    // Get column name and type
//                    var columnName = ParserName.Parse(param.Name, "`").Unquoted().Normalized().ToString();
//                    var syncColumn = new SyncColumn(columnName)
//                    {
//                        DbType = (int)param.DbType.Value,
//                        MaxLength = param.MaxLength,
//                    };
//                    var sqlDbType = this.MySqlDbMetadata.GetOwnerDbTypeFromDbType(syncColumn);

//                    var customParameterFilter = new MySqlParameter($"in_{columnName}", sqlDbType)
//                    {
//                        Size = param.MaxLength,
//                        IsNullable = param.AllowNull,
//                        Value = param.DefaultValue
//                    };
//                    command.Parameters.Add(customParameterFilter);
//                }
//                else
//                {
//                    var tableFilter = this.TableDescription.Schema.Tables[param.TableName, param.SchemaName];
//                    if (tableFilter == null)
//                        throw new FilterParamTableNotExistsException(param.TableName);

//                    var columnFilter = tableFilter.Columns[param.Name];
//                    if (columnFilter == null)
//                        throw new FilterParamColumnNotExistsException(param.Name, param.TableName);

//                    // Get column name and type
//                    var columnName = ParserName.Parse(columnFilter, "`").Unquoted().Normalized().ToString();


//                    var sqlDbType = tableFilter.OriginalProvider == originalProvider ?
//                        this.MySqlDbMetadata.GetMySqlDbType(columnFilter) : this.MySqlDbMetadata.GetOwnerDbTypeFromDbType(columnFilter);

//                    // Add it as parameter
//                    var sqlParamFilter = new MySqlParameter($"in_{columnName}", sqlDbType)
//                    {
//                        Size = columnFilter.MaxLength,
//                        IsNullable = param.AllowNull,
//                        Value = param.DefaultValue
//                    };
//                    command.Parameters.Add(sqlParamFilter);
//                }

//            }

//        }

        public override Task ExecuteBatchCommandAsync(DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> arrayItems, SyncTable schemaChangesTable, SyncTable failedRows, long? lastTimestamp, DbConnection connection, DbTransaction transaction = null) 
            => throw new NotImplementedException();
    }
}
