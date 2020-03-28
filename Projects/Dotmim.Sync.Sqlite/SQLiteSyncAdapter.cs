using System;
using System.Collections.Generic;
using System.Linq;

using System.Data.Common;
using System.Data;
using Dotmim.Sync.Builders;
using Microsoft.Data.Sqlite;
using System.Threading.Tasks;

namespace Dotmim.Sync.Sqlite
{
    public class SqliteSyncAdapter : DbSyncAdapter
    {
        private SqliteConnection connection;
        private SqliteTransaction transaction;
        private SqliteObjectNames sqliteObjectNames;
        private SqliteDbMetadata sqliteDbMetadata;

        public override DbConnection Connection
        {
            get
            {
                return this.connection;
            }
        }
        public override DbTransaction Transaction
        {
            get
            {
                return this.transaction;
            }

        }

        public SqliteSyncAdapter(SyncTable tableDescription, DbConnection connection, DbTransaction transaction) : base(tableDescription)
        {
            var sqlc = connection as SqliteConnection;
            this.connection = sqlc ?? throw new InvalidCastException("Connection should be a SqliteConnection");

            this.transaction = transaction as SqliteTransaction;

            this.sqliteObjectNames = new SqliteObjectNames(TableDescription);
            this.sqliteDbMetadata = new SqliteDbMetadata();
        }

        public override bool IsPrimaryKeyViolation(Exception Error)
        {
            return false;
        }

        public override DbCommand GetCommand(DbCommandType commandType, SyncFilter filter = null)
        {
            var command = this.Connection.CreateCommand();
            string text;
            text = this.sqliteObjectNames.GetCommandName(commandType, filter);

            // on Sqlite, everything is text :)
            command.CommandType = CommandType.Text;
            command.CommandText = text;
            command.Connection = Connection;

            if (Transaction != null)
                command.Transaction = Transaction;

            return command;
        }

        public override Task SetCommandParametersAsync(DbCommandType commandType, DbCommand command, SyncFilter filter = null)
        {
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

        public override Task ExecuteBatchCommandAsync(DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> applyRows, SyncTable schemaChangesTable, SyncTable failedRows, long lastTimestamp) 
            => throw new NotImplementedException();

        public override bool IsUniqueKeyViolation(Exception exception)
        {
            return false;
        }
    }
}
