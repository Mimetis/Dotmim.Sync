using System;
using System.Collections.Generic;
using System.Linq;
using Dotmim.Sync.Data;
using System.Data.Common;
using System.Data;
using Dotmim.Sync.Builders;
using Microsoft.Data.Sqlite;
using Dotmim.Sync.Filter;

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

        public override DbCommand GetCommand(DbCommandType commandType, IEnumerable<SyncFilter> filters = null)
        {
            var command = this.Connection.CreateCommand();
            string text;

            if (filters != null)
                text = this.sqliteObjectNames.GetCommandName(commandType, filters);
            else
                text = this.sqliteObjectNames.GetCommandName(commandType);

            // on Sqlite, everything is text :)
            command.CommandType = CommandType.Text;
            command.CommandText = text;
            command.Connection = Connection;

            if (Transaction != null)
                command.Transaction = Transaction;

            return command;
        }

        public override void SetCommandParameters(DbCommandType commandType, DbCommand command, IEnumerable<SyncFilter> filters = null)
        {
            switch (commandType)
            {
                case DbCommandType.SelectChanges:
                case DbCommandType.SelectChangesWitFilters:
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
                case DbCommandType.InsertMetadata:
                    this.SetInsertMetadataParameters(command);
                    break;
                case DbCommandType.InsertRow:
                    this.SetInsertRowParameters(command);
                    break;
                case DbCommandType.UpdateMetadata:
                    this.SetUpdateMetadataParameters(command);
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
            p.ParameterName = "@update_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_row_is_tombstone";
            p.DbType = DbType.Int32;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@update_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);
        }

        private void SetInsertRowParameters(DbCommand command)
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
        }

        private void SetInsertMetadataParameters(DbCommand command)
        {
            DbParameter p;

            foreach (var column in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var unquotedColumn = ParserName.Parse(column).Normalized().Unquoted().ToString();
                p = command.CreateParameter();
                p.ParameterName = $"@{unquotedColumn}";
                p.DbType = column.GetDbType();
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = "@create_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@update_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_row_is_tombstone";
            p.DbType = DbType.Int32;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@create_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@update_timestamp";
            p.DbType = DbType.Int64;
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
            return;
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

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_is_new";
            p.DbType = DbType.Boolean;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_is_reinit";
            p.DbType = DbType.Boolean;
            command.Parameters.Add(p);
        }

        public override void ExecuteBatchCommand(DbCommand cmd, IEnumerable<SyncRow> applyRows, SyncTable schemaChangesTable, SyncTable failedRows, Guid applyingScopeId, long lastTimestamp)
        {
            throw new NotImplementedException();
        }

        public override bool IsUniqueKeyViolation(Exception exception)
        {
            return false;
        }
    }
}
