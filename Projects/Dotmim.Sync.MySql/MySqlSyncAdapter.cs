using System;
using System.Collections.Generic;
using System.Linq;
using Dotmim.Sync.Data;
using System.Data.Common;
using System.Data;
using Dotmim.Sync.Builders;
using MySql.Data.MySqlClient;

namespace Dotmim.Sync.MySql
{
    public class MySqlSyncAdapter : DbSyncAdapter
    {
        private MySqlConnection connection;
        private MySqlTransaction transaction;
        private MySqlObjectNames mySqlObjectNames;
        // Derive Parameters cache
        private static Dictionary<string, List<MySqlParameter>> derivingParameters = new Dictionary<string, List<MySqlParameter>>();

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

        public MySqlSyncAdapter(DmTable tableDescription, DbConnection connection, DbTransaction transaction) : base(tableDescription)
        {
            var sqlc = connection as MySqlConnection;
            this.connection = sqlc ?? throw new InvalidCastException("Connection should be a MySqlConnection");

            this.transaction = transaction as MySqlTransaction;

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


        public override DbCommand GetCommand(DbCommandType commandType, IEnumerable<string> additionals = null)
        {
            var command = this.Connection.CreateCommand();
            string text;

            if (additionals != null)
                text = this.mySqlObjectNames.GetCommandName(commandType, additionals);
            else
                text = this.mySqlObjectNames.GetCommandName(commandType);

            var textName = new ObjectNameParser(text, "`", "`");
            // on MySql, everything is text based :)
            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = textName.ObjectName;
            command.Connection = Connection;

            //if (commandType == DbCommandType.UpdateRow)
            //{
            //    command.CommandType = CommandType.StoredProcedure;
            //    command.CommandText = "customers_update";
            //    command.Connection = Connection;
            //}

            if (Transaction != null)
                command.Transaction = Transaction;

            return command;
        }


        public override void SetCommandParameters(DbCommandType commandType, DbCommand command)
        {
            switch (commandType)
            {
                case DbCommandType.SelectChanges:
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
                default:
                    break;
            }
        }

        private void SetUpdateRowParameters(DbCommand command)
        {
            DbParameter p;

            foreach (DmColumn column in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName);
                p = command.CreateParameter();
                p.ParameterName = $"{MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER}{quotedColumn.UnquotedStringWithUnderScore}";
                p.DbType = column.DbType;
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

        private void SetUpdateMetadataParameters(DbCommand command)
        {
            DbParameter p;

            foreach (DmColumn column in this.TableDescription.PrimaryKey.Columns.Where(c => !c.IsReadOnly))
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName);
                p = command.CreateParameter();
                p.ParameterName = $"{MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER}{quotedColumn.UnquotedStringWithUnderScore}";
                p.DbType = column.DbType;
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

        private void SetInsertRowParameters(DbCommand command)
        {
            DbParameter p;

            foreach (DmColumn column in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName);
                p = command.CreateParameter();
                p.ParameterName = $"{MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER}{quotedColumn.UnquotedStringWithUnderScore}";
                p.DbType = column.DbType;
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }
        }

        private void SetInsertMetadataParameters(DbCommand command)
        {
            DbParameter p;

            foreach (DmColumn column in this.TableDescription.PrimaryKey.Columns.Where(c => !c.IsReadOnly))
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName);
                p = command.CreateParameter();
                p.ParameterName = $"{MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER}{quotedColumn.UnquotedStringWithUnderScore}";
                p.DbType = column.DbType;
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

            foreach (DmColumn column in this.TableDescription.PrimaryKey.Columns.Where(c => !c.IsReadOnly))
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName);
                p = command.CreateParameter();
                p.ParameterName = $"{MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER}{quotedColumn.UnquotedStringWithUnderScore}";
                p.DbType = column.DbType;
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

            foreach (DmColumn column in this.TableDescription.PrimaryKey.Columns.Where(c => !c.IsReadOnly))
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName);
                p = command.CreateParameter();
                p.ParameterName = $"{MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER}{quotedColumn.UnquotedStringWithUnderScore}";
                p.DbType = column.DbType;
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

            foreach (DmColumn column in this.TableDescription.PrimaryKey.Columns.Where(c => !c.IsReadOnly))
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName);
                p = command.CreateParameter();
                p.ParameterName = $"{MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER}{quotedColumn.UnquotedStringWithUnderScore}";
                p.DbType = column.DbType;
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

        private void SetSelecteChangesParameters(DbCommand command)
        {
            var p = command.CreateParameter();
            p.ParameterName = "sync_min_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "sync_scope_is_new";
            p.DbType = DbType.Boolean;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "sync_scope_is_reinit";
            p.DbType = DbType.Boolean;
            command.Parameters.Add(p);
        }

        public override void ExecuteBatchCommand(DbCommand cmd, DmView applyTable, DmTable failedRows, ScopeInfo scope)
        {
            throw new NotImplementedException();
        }

    }
}
