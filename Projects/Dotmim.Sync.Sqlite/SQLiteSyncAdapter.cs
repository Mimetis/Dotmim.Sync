using System;
using System.Collections.Generic;
using System.Linq;

using System.Data.Common;
using System.Data;
using Dotmim.Sync.Builders;
using Microsoft.Data.Sqlite;
using System.Threading.Tasks;
using Dotmim.Sync.Sqlite.Builders;

namespace Dotmim.Sync.Sqlite
{
    public class SqliteSyncAdapter : SyncAdapter
    {
        private SqliteObjectNames sqliteObjectNames;
        private ParserName tableName;
        private ParserName trackingName;
        private SyncSetup setup;
        private SqliteBuilderCommands sqlBuilderCommands;
        private SqliteDbMetadata sqliteDbMetadata;

        public SqliteSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup, string scopeName) 
            : base(tableDescription, setup, scopeName)
        {
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.setup = setup;
            this.sqlBuilderCommands = new SqliteBuilderCommands(tableDescription, tableName, trackingName, setup);
            this.sqliteDbMetadata = new SqliteDbMetadata();
        }

        public override bool IsPrimaryKeyViolation(Exception Error) => false;




        public override Task<string> PreExecuteCommandAsync(DbCommandType commandType, DbConnection connection, DbTransaction transaction)
            => null;

        public override Task PostExecuteCommandAsync(DbCommandType commandType, string optionalState, DbConnection connection, DbTransaction transaction)
            => Task.CompletedTask;

        public override DbCommand GetCommand(DbCommandType commandType, SyncFilter filter)
        {
            var command = new SqliteCommand();

            string commandText = null;

            switch (commandType)
            {
                case DbCommandType.SelectChanges:
                case DbCommandType.SelectChangesWithFilters:
                    commandText = this.sqlBuilderCommands.GetSelectChangesCommandText();
                    break;
                case DbCommandType.SelectInitializedChanges:
                case DbCommandType.SelectInitializedChangesWithFilters:
                    commandText = this.sqlBuilderCommands.GetSelectInitializeChangesCommandText();
                    break;
                case DbCommandType.SelectRow:
                    commandText = this.sqlBuilderCommands.GetSelectRowCommandText();
                    break;
                case DbCommandType.UpdateRow:
                    commandText = this.sqlBuilderCommands.GetUpdateCommandText();
                    break;
                case DbCommandType.UpdateBatchRows:
                    commandText = this.sqlBuilderCommands.GetUpdateBulkCommandText();
                    break;
                case DbCommandType.DeleteRow:
                    commandText = this.sqlBuilderCommands.GetDeleteRowCommandText();
                    break;
                case DbCommandType.DeleteBatchRows:
                    commandText = this.sqlBuilderCommands.GetDeleteBulkCommandText();
                    break;
                case DbCommandType.DisableConstraints:
                    commandText = this.sqlBuilderCommands.GetDisableConstraintsCommandText();
                    break;
                case DbCommandType.EnableConstraints:
                    commandText = this.sqlBuilderCommands.GetEnableConstraintsCommandText();
                    break;
                case DbCommandType.DeleteMetadata:
                    commandText = this.sqlBuilderCommands.GetDeleteMetadataRowCommandText();
                    break;
                case DbCommandType.UpdateMetadata:
                    commandText = this.sqlBuilderCommands.GetUpdateMetadataRowCommandText();
                    break;
                case DbCommandType.UpdateUntrackedRows:
                    commandText = this.sqlBuilderCommands.GetUpdateUntrackedRowsCommandText();
                    break;
                case DbCommandType.Reset:
                    commandText = this.sqlBuilderCommands.GetResetCommandText();
                    break;
                default:
                    break;
            }

            command.CommandType = CommandType.Text;
            command.CommandText = commandText;

            return command;
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
                    this.SetSelecteChangesParameters(command);
                    break;
                case DbCommandType.SelectRow:
                    this.SetSelectRowParameters(command);
                    break;
                case DbCommandType.DeleteMetadata:
                    this.SetDeleteMetadataParameters(command);
                    break;
                case DbCommandType.DeleteRow:
                    this.SetDeleteRowParameters(command);
                    break;
                case DbCommandType.UpdateRow:
                    this.SetUpdateRowParameters(command);
                    break;
                case DbCommandType.Reset:
                    this.SetResetParameters(command);
                    break;
                case DbCommandType.UpdateMetadata:
                    this.SetUpdateMetadataParameters(command);
                    break;
                case DbCommandType.UpdateUntrackedRows:
                    throw new NotImplementedException();
                case DbCommandType.SelectInitializedChanges:
                case DbCommandType.SelectInitializedChangesWithFilters:
                case DbCommandType.UpdateBatchRows:
                case DbCommandType.DeleteBatchRows:
                case DbCommandType.DisableConstraints:
                case DbCommandType.EnableConstraints:
                default:
                    break;
            }

            return Task.CompletedTask;
        }

        private void SetResetParameters(DbCommand command)
        {
            // nothing to set here
        }

        private DbType GetValidDbType(DbType dbType)
        {
            if (dbType == DbType.Time)
                return DbType.String;

            if (dbType == DbType.Object)
                return DbType.String;

            return dbType;
        }

        private void SetUpdateRowParameters(DbCommand command)
        {
            DbParameter p;

            foreach (var column in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var unquotedColumn = ParserName.Parse(column).Normalized().Unquoted().ToString();
                p = command.CreateParameter();
                p.ParameterName = $"@{unquotedColumn}";
                p.DbType = GetValidDbType(column.GetDbType());
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = "@sync_force_write";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_min_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);



        }

        private void SetDeleteRowParameters(DbCommand command)
        {
            DbParameter p;

            foreach (var column in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var unquotedColumn = ParserName.Parse(column).Normalized().Unquoted().ToString();
                p = command.CreateParameter();
                p.ParameterName = $"@{unquotedColumn}";
                p.DbType = GetValidDbType(column.GetDbType());
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = "@sync_force_write";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_min_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

        }

        private void SetSelectRowParameters(DbCommand command)
        {
            DbParameter p;

            foreach (var column in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var unquotedColumn = ParserName.Parse(column).Normalized().Unquoted().ToString();
                p = command.CreateParameter();
                p.ParameterName = $"@{unquotedColumn}";
                p.DbType = GetValidDbType(column.GetDbType());
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

        }

        private void SetUpdateMetadataParameters(DbCommand command)
        {
            DbParameter p;

            foreach (var column in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var unquotedColumn = ParserName.Parse(column).Normalized().Unquoted().ToString();
                p = command.CreateParameter();
                p.ParameterName = $"@{unquotedColumn}";
                p.DbType = GetValidDbType(column.GetDbType());
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_row_is_tombstone";
            p.DbType = DbType.Boolean;
            command.Parameters.Add(p);

        }
        private void SetDeleteMetadataParameters(DbCommand command)
        {
            var p = command.CreateParameter();
            p.ParameterName = "@sync_row_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);
        }

        private void SetSelecteChangesParameters(DbCommand command)
        {
            var p = command.CreateParameter();
            p.ParameterName = "@sync_min_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);
        }

        public override bool IsUniqueKeyViolation(Exception exception) => false;

        public override Task ExecuteBatchCommandAsync(DbCommandType commandType, Guid senderScopeId, IEnumerable<SyncRow> arrayItems, SyncTable schemaChangesTable, SyncTable failedRows, long lastTimestamp, string optionalState, DbConnection connection, DbTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public override Task<string> PreExecuteBatchCommandAsync(DbCommandType commandType, Guid senderScopeId, SyncTable schemaChangesTable, SyncTable failedRows, long lastTimestamp, DbConnection connection, DbTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public override Task PostExecuteBatchCommandAsync(DbCommandType commandType, Guid senderScopeId, SyncTable schemaChangesTable, SyncTable failedRows, long lastTimestamp, string optionalState, DbConnection connection, DbTransaction transaction)
        {
            throw new NotImplementedException();
        }
    }
}
