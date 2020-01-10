using System;
using System.Collections.Generic;
using System.Linq;
using Dotmim.Sync.Data;
using System.Data.Common;
using System.Data;
using Dotmim.Sync.Builders;
using MySql.Data.MySqlClient;
using Dotmim.Sync.MySql.Builders;
using Dotmim.Sync.Filter;

namespace Dotmim.Sync.MySql
{
    public class MySqlSyncAdapter : DbSyncAdapter
    {
        private MySqlConnection connection;
        private MySqlTransaction transaction;
        private MySqlObjectNames mySqlObjectNames;
        private MySqlDbMetadata mySqlDbMetadata;


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

        public MySqlSyncAdapter(SyncTable tableDescription, DbConnection connection, DbTransaction transaction) : base(tableDescription)
        {
            var sqlc = connection as MySqlConnection;
            this.connection = sqlc ?? throw new InvalidCastException("Connection should be a MySqlConnection");

            this.transaction = transaction as MySqlTransaction;
            this.mySqlDbMetadata = new MySqlDbMetadata();

            this.mySqlObjectNames = new MySqlObjectNames(TableDescription);
        }

        public override bool IsPrimaryKeyViolation(Exception Error)
        {
            return false;
        }
        public override bool IsUniqueKeyViolation(Exception exception)
        {
            return false;
        }


        public override DbCommand GetCommand(DbCommandType commandType, IEnumerable<SyncFilter> additionals = null)
        {
            var command = this.Connection.CreateCommand();
            string text;
            bool isStoredProc;

            if (additionals != null && additionals.Count() > 0)
                (text, isStoredProc) = this.mySqlObjectNames.GetCommandName(commandType, additionals);
            else
                (text, isStoredProc) = this.mySqlObjectNames.GetCommandName(commandType);

            var textName = ParserName.Parse(text, "`");

            command.CommandType = isStoredProc ? CommandType.StoredProcedure : CommandType.Text;
            command.CommandText = isStoredProc ? textName.Quoted().ToString() : text;
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
                case DbCommandType.SelectChangesWithFilters:
                case DbCommandType.SelectInitializedChanges:
                case DbCommandType.SelectInitializedChangesWithFilters:
                    this.SetSelecteChangesParameters(command, filters);
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
                case DbCommandType.UpdateMetadata:
                    this.SetUpdateMetadataParameters(command);
                    break;
                case DbCommandType.UpdateRow:
                    this.SetUpdateRowParameters(command);
                    break;
                default:
                    break;
            }
        }

        private void SetUpdateRowParameters(DbCommand command)
        {
            DbParameter p;

            foreach (var column in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(column, "`").Unquoted().Normalized().ToString();

                p = command.CreateParameter();
                p.ParameterName = $"{MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER}{columnName}";
                p.DbType = column.GetDbType();
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = "sync_force_write";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "sync_min_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

        }

        private void SetUpdateMetadataParameters(DbCommand command)
        {
            DbParameter p;

            foreach (var column in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var quotedColumn = ParserName.Parse(column, "`").Unquoted().Normalized().ToString();
                p = command.CreateParameter();
                p.ParameterName = $"{MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER}{quotedColumn}";
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
            p.DbType = DbType.Int32;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "create_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "update_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);
        }
      
        private void SetDeleteRowParameters(DbCommand command)
        {
            DbParameter p;

            foreach (var column in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var quotedColumn = ParserName.Parse(column, "`").Unquoted().Normalized().ToString();

                p = command.CreateParameter();
                p.ParameterName = $"{MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER}{quotedColumn}";
                p.DbType = column.GetDbType();
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = "sync_force_write";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "sync_min_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);
        }

        private void SetSelectRowParameters(DbCommand command)
        {
            DbParameter p;

            foreach (var column in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var quotedColumn = ParserName.Parse(column, "`").Unquoted().Normalized().ToString();

                p = command.CreateParameter();
                p.ParameterName = $"{MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER}{quotedColumn}";
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
            DbParameter p;

            foreach (var column in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var quotedColumn = ParserName.Parse(column, "`").Unquoted().Normalized().ToString();

                p = command.CreateParameter();
                p.ParameterName = $"{MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER}{quotedColumn}";
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
            p.DbType = DbType.Int32;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "create_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "update_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);
        }

        private void SetSelecteChangesParameters(DbCommand command, IEnumerable<SyncFilter> filters = null)
        {
            var p = command.CreateParameter();
            p.ParameterName = "sync_min_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            if (filters != null)
            {
                foreach (var filter in filters)
                {
                    if (!filter.IsVirtual)
                    {
                        var columnFilter = this.TableDescription.Columns[filter.ColumnName];

                        if (columnFilter == null)
                            throw new InvalidExpressionException($"Column {filter.ColumnName} does not exist in Table {this.TableDescription.TableName}");

                        var columnName = ParserName.Parse(columnFilter).Unquoted().Normalized().ToString();
                        var mySqlDbType = (MySqlDbType)this.mySqlDbMetadata.TryGetOwnerDbType(columnFilter.OriginalDbType, columnFilter.GetDbType(), false, false, columnFilter.MaxLength, this.TableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);
                        var mySqlParamFilter = new MySqlParameter($"{MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER}{columnName}", mySqlDbType);
                        command.Parameters.Add(mySqlParamFilter);
                    }
                    else
                    {
                        var mySqlDbType = (MySqlDbType)this.mySqlDbMetadata.TryGetOwnerDbType(null, (DbType)filter.ColumnType.Value, false, false, 0, this.TableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);
                        var columnFilterName = ParserName.Parse(filter.ColumnName).Unquoted().Normalized().ToString();
                        var mySqlParamFilter = new MySqlParameter($"{MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER}{columnFilterName}", mySqlDbType);
                        command.Parameters.Add(mySqlParamFilter);
                    }
                }
            }

        }


        public override void ExecuteBatchCommand(DbCommand cmd, IEnumerable<SyncRow> applyRows, SyncTable schemaChangesTable, SyncTable failedRows, Guid applyingScopeId, long lastTimestamp)
        {
            throw new NotImplementedException();
        }

    }
}
