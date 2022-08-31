using Dotmim.Sync.Builders;
using Dotmim.Sync.Manager;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.SqlServer.Builders;
using Dotmim.Sync.SqlServer.Manager;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.SampleConsole
{

    /// <summary>
    /// Use this provider if your client database does not need to upload any data to the server.
    /// This provider does not create any triggers / tracking tables and only 3 stored proc / tables
    /// If your client database is not readonly, any changes from server will overwrite client changes
    /// </summary>
    public class SqlSyncDownloadOnlyProvider : SqlSyncProvider
    {
        public SqlSyncDownloadOnlyProvider() { }
        public SqlSyncDownloadOnlyProvider(string connectionString) : base(connectionString) { }
        public SqlSyncDownloadOnlyProvider(SqlConnectionStringBuilder builder) : base(builder) { }

        public override DbSyncAdapter GetSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
            => new SqlDownloadOnlySyncAdapter(tableDescription, tableName, trackingTableName, setup, scopeName, this.UseBulkOperations);

        public override DbTableBuilder GetTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
            => new SqlDownloadOnlyTableBuilder(tableDescription, tableName, trackingTableName, setup, scopeName);
    }

    /// <summary>
    /// Table builder builds table, tracking tables, triggers, stored proc, types
    /// </summary>
    public class SqlDownloadOnlyTableBuilder : SqlTableBuilder
    {
        public SqlDownloadOnlyTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName) 
            : base(tableDescription, tableName, trackingTableName, setup, scopeName){}

        public override Task<DbCommand> GetCreateStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            var command = storedProcedureType switch
            {
                DbStoredProcedureType.BulkTableType => this.CreateBulkTableTypeCommand(connection, transaction),
                DbStoredProcedureType.BulkUpdateRows => this.CreateBulkUpdateCommand(connection, transaction),
                DbStoredProcedureType.BulkDeleteRows => this.CreateBulkDeleteCommand(connection, transaction),
                DbStoredProcedureType.Reset => this.CreateResetCommand(connection, transaction),
                _ => null
            };

            return Task.FromResult(command);
        }
        public override Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => Task.FromResult<DbCommand>(null);
        public override Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => Task.FromResult<DbCommand>(null);
        public override Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => Task.FromResult<DbCommand>(null);
        public override Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => Task.FromResult<DbCommand>(null);
        public override Task<DbCommand> GetExistsTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => Task.FromResult<DbCommand>(null);
        public override Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => Task.FromResult<DbCommand>(null);
        public override Task<DbCommand> GetRenameTrackingTableCommandAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction)
            => Task.FromResult<DbCommand>(null);

        private DbCommand CreateBulkTableTypeCommand(DbConnection connection, DbTransaction transaction)
        {
            StringBuilder stringBuilder = new StringBuilder();
            var commandName = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkTableType);

            stringBuilder.AppendLine($"CREATE TYPE {commandName} AS TABLE (");
            string str = "";

            foreach (var c in this.TableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var isPrimaryKey = this.TableDescription.IsPrimaryKey(c.ColumnName);

                var columnName = ParserName.Parse(c).Quoted().ToString();
                var nullString = isPrimaryKey ? "NOT NULL" : "NULL";

                // Get the good SqlDbType (even if we are not from Sql Server def)
                var columnType = this.SqlDbMetadata.GetCompatibleColumnTypeDeclarationString(c, this.TableDescription.OriginalProvider);

                stringBuilder.AppendLine($"{str}{columnName} {columnType} {nullString}");
                str = ", ";
            }
            //stringBuilder.AppendLine(", [update_scope_id] [uniqueidentifier] NULL");
            stringBuilder.Append(string.Concat(str, "PRIMARY KEY ("));
            str = "";
            foreach (var c in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"{str}{columnName} ASC");
                str = ", ";
            }

            stringBuilder.Append("))");
            var command = new SqlCommand(stringBuilder.ToString(), (SqlConnection)connection, (SqlTransaction)transaction);
            return command;
        }
        private DbCommand CreateResetCommand(DbConnection connection, DbTransaction transaction)
        {
            var procName = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.Reset);
            StringBuilder stringBuilder = new StringBuilder(string.Concat("CREATE PROCEDURE ", procName));

            SqlParameter sqlParameter = new SqlParameter("@sync_row_count", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output
            };
            string str = "\n\t";
            stringBuilder.Append(string.Concat(str, SqlBuilderProcedure.CreateParameterDeclaration(sqlParameter)));

            stringBuilder.Append("\nAS\nBEGIN\n");
            stringBuilder.AppendLine($"DELETE FROM {this.TableName.Schema().Quoted().ToString()};");
            stringBuilder.AppendLine(string.Concat("SET ", sqlParameter.ParameterName, " = @@ROWCOUNT;"));
            stringBuilder.AppendLine("END");

            var sqlCommand = new SqlCommand(stringBuilder.ToString(), (SqlConnection)connection, (SqlTransaction)transaction);
            return sqlCommand;
        }
        private DbCommand CreateBulkDeleteCommand(DbConnection connection, DbTransaction transaction)
        {
            var procName = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkDeleteRows);

            StringBuilder stringBuilder = new StringBuilder(string.Concat("CREATE PROCEDURE ", procName));

            string str = "\n\t";

            var sqlParameterChangeTable = new SqlParameter("@changeTable", SqlDbType.Structured)
            {
                TypeName = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkTableType)
            };
            stringBuilder.Append(string.Concat(str, SqlBuilderProcedure.CreateParameterDeclaration(sqlParameterChangeTable)));

            stringBuilder.Append("\nAS\nBEGIN\n");

            string joins = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[changes]", "[base]");

            stringBuilder.AppendLine($"DELETE {this.TableName.Schema().Quoted()}");
            stringBuilder.AppendLine($"FROM {this.TableName.Quoted()} [base]");
            stringBuilder.AppendLine($"JOIN @changeTable as [changes] ON {joins}");

            stringBuilder.AppendLine("END");

            var sqlCommand = new SqlCommand(stringBuilder.ToString(), (SqlConnection)connection, (SqlTransaction)transaction);
            return sqlCommand;
        }
        private DbCommand CreateBulkUpdateCommand(DbConnection connection, DbTransaction transaction)
        {
            var procName = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkUpdateRows);

            StringBuilder stringBuilder = new StringBuilder(string.Concat("CREATE PROCEDURE ", procName));
            string str = "\n\t";

            var sqlParameterChangeTable = new SqlParameter("@changeTable", SqlDbType.Structured)
            {
                TypeName = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkTableType)
            };
            stringBuilder.Append(string.Concat(str, SqlBuilderProcedure.CreateParameterDeclaration(sqlParameterChangeTable)));

            stringBuilder.Append("\nAS\nBEGIN\n");

            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();
            string empty = string.Empty;

            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[p]", "[t]");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[changes]", "[base]");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[t]", "[side]");
            string str7 = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[p]", "[side]");


            // Check if we have auto inc column
            if (this.TableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {this.TableName.Schema().Quoted().ToString()} ON;");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine($"MERGE {this.TableName.Schema().Quoted().ToString()} AS [base]");
            stringBuilder.AppendLine($"USING @changeTable as [changes] on {str5}");

            var hasMutableColumns = this.TableDescription.GetMutableColumns(false).Any();

            if (hasMutableColumns)
            {
                stringBuilder.AppendLine("WHEN MATCHED THEN");
                foreach (var mutableColumn in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
                {
                    var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                    stringBuilderArguments.Append(string.Concat(empty, columnName));
                    stringBuilderParameters.Append(string.Concat(empty, $"changes.{columnName}"));
                    empty = ", ";
                }
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"\tUPDATE SET");

                string strSeparator = "";
                foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false))
                {
                    var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                    stringBuilder.AppendLine($"\t{strSeparator}{columnName} = [changes].{columnName}");
                    strSeparator = ", ";
                }
            }

            stringBuilder.AppendLine("WHEN NOT MATCHED BY TARGET THEN");

            stringBuilderArguments = new StringBuilder();
            stringBuilderParameters = new StringBuilder();
            empty = string.Empty;

            foreach (var mutableColumn in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();

                stringBuilderArguments.Append(string.Concat(empty, columnName));
                stringBuilderParameters.Append(string.Concat(empty, $"[changes].{columnName}"));
                empty = ", ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\tINSERT");
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()});");

            // Check if we have auto inc column
            if (this.TableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {this.TableName.Schema().Quoted().ToString()} ON;");
                stringBuilder.AppendLine();
            }

            stringBuilder.Append("\nEND");

            var command = new SqlCommand(stringBuilder.ToString(), (SqlConnection)connection, (SqlTransaction)transaction);
            return command;

        }


    }

    /// <summary>
    /// Sync Adapter gets and executes commands
    /// </summary>
    public class SqlDownloadOnlySyncAdapter : SqlSyncAdapter
    {
        public SqlDownloadOnlySyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup, string scopeName, bool useBulkOperations)
            : base(tableDescription, tableName, trackingName, setup, scopeName, useBulkOperations) { }

        /// <summary>
        /// Returning null for all non used commands (from case default)
        /// </summary>
        public override (DbCommand, bool) GetCommand(DbCommandType nameType, SyncFilter filter)
        {
            var isBatch = false;
            var command = new SqlCommand();
            switch (nameType)
            {
                case DbCommandType.DisableConstraints:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.SqlObjectNames.GetCommandName(DbCommandType.DisableConstraints, filter);
                    break;
                case DbCommandType.EnableConstraints:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.SqlObjectNames.GetCommandName(DbCommandType.EnableConstraints, filter);
                    break;
                case DbCommandType.BulkTableType:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkTableType, filter);
                    break;
                case DbCommandType.UpdateRow:
                case DbCommandType.InsertRow:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkUpdateRows, filter);
                    isBatch = true;
                    break;
                case DbCommandType.DeleteRow:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkDeleteRows, filter);
                    isBatch = true;
                    break;
                case DbCommandType.Reset:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.Reset, filter);
                    break;
                default:
                    return (null, false);
            }

            return (command, isBatch);
        }

    }
}
