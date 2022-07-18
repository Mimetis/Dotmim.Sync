using Dotmim.Sync;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Manager;
using Dotmim.Sync.MySql;
using Dotmim.Sync.MySql.Builders;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.SqlServer.Builders;
using Dotmim.Sync.SqlServer.Manager;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CustomProvider
{

    /// <summary>
    /// Use this provider if your client database does not need to upload any data to the server.
    /// This provider does not create any triggers / tracking tables and only 3 stored proc / tables
    /// If your client database is not readonly, any changes from server will overwrite client changes
    /// </summary>
    public class MySqlSyncDownloadOnlyProvider : MySqlSyncProvider
    {
        public MySqlSyncDownloadOnlyProvider() { }
        public MySqlSyncDownloadOnlyProvider(string connectionString) : base(connectionString) { }
        public MySqlSyncDownloadOnlyProvider(MySqlConnectionStringBuilder builder) : base(builder) { }

        public override DbSyncAdapter GetSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
            => new MySqlDownloadOnlySyncAdapter(tableDescription, tableName, trackingTableName, setup, scopeName);

        public override DbTableBuilder GetTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
            => new MySqlDownloadOnlyTableBuilder(tableDescription, tableName, trackingTableName, setup, scopeName);
    }

    /// <summary>
    /// Table builder builds table, tracking tables, triggers, stored proc, types
    /// </summary>
    public class MySqlDownloadOnlyTableBuilder : MySqlTableBuilder
    {
        public MySqlDownloadOnlyTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
            : base(tableDescription, tableName, trackingTableName, setup, scopeName) { }

        public override Task<DbCommand> GetCreateStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => Task.FromResult<DbCommand>(null);
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
    }

    /// <summary>
    /// Sync Adapter gets and executes commands
    /// </summary>
    public class MySqlDownloadOnlySyncAdapter : MySqlSyncAdapter
    {
        private ParserName tableName;

        public MySqlDownloadOnlySyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup, string scopeName)
            : base(tableDescription, tableName, trackingName, setup, scopeName)
        {
            this.tableName = tableName;
        }

        /// <summary>
        /// Returning null for all non used commands (from case default)
        /// </summary>
        public override (DbCommand, bool) GetCommand(DbCommandType nameType, SyncFilter filter)
        {
            var command = new MySqlCommand();
            switch (nameType)
            {
                case DbCommandType.UpdateRow:
                case DbCommandType.InsertRow:
                    command = CreateUpdateCommand();
                    break;
                case DbCommandType.DeleteRow:
                    command = CreateDeleteCommand();
                    break;
                case DbCommandType.DisableConstraints:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.MySqlObjectNames.GetCommandName(DbCommandType.DisableConstraints, filter);
                    break;
                case DbCommandType.EnableConstraints:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.MySqlObjectNames.GetCommandName(DbCommandType.EnableConstraints, filter);
                    break;
                case DbCommandType.Reset:
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = this.MySqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.Reset, filter);
                    break;
                default:
                    return (null, false);
            }

            return (command, false);
        }

        public override Task AddCommandParametersAsync(DbCommandType commandType, DbCommand command, DbConnection connection, DbTransaction transaction = null, SyncFilter filter = null)
        {

            if (command == null)
                return Task.CompletedTask;

            if (command.Parameters != null && command.Parameters.Count > 0)
                return Task.CompletedTask;

            switch (commandType)
            {
                case DbCommandType.DeleteRow:
                    this.SetDeleteRowParameters(command);
                    return Task.CompletedTask; ;
                case DbCommandType.UpdateRow:
                case DbCommandType.InsertRow:
                    this.SetUpdateRowParameters(command);
                    return Task.CompletedTask; ;
                default:
                    break;
            }

            return base.AddCommandParametersAsync(commandType, command, connection, transaction, filter);
        }

        private MySqlCommand CreateUpdateCommand()
        {
            var mySqlCommand = new MySqlCommand();
            var stringBuilder = new StringBuilder();
            var hasMutableColumns = this.TableDescription.GetMutableColumns(false).Any();

            var selectAllColumnsString = new StringBuilder();
            var setUpdateAllColumnsString = new StringBuilder();

            string empty = string.Empty;
            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(true, true))
            {
                var mutableColumnName = ParserName.Parse(mutableColumn, "`").Quoted().ToString();
                var parameterColumnName = ParserName.Parse(mutableColumn, "`").Unquoted().Normalized().ToString();

                selectAllColumnsString.Append($"{empty}@{parameterColumnName} as {mutableColumnName}");
                setUpdateAllColumnsString.Append($"{empty}{mutableColumnName}=`side`.{mutableColumnName}");
                empty = ", ";
            }

            stringBuilder.AppendLine($"INSERT INTO {tableName.Quoted()}");
            stringBuilder.AppendLine($"SELECT * FROM (SELECT {selectAllColumnsString}) as `side`");
            if (hasMutableColumns)
            {
                stringBuilder.AppendLine($"ON DUPLICATE KEY");
                stringBuilder.AppendLine($"UPDATE {setUpdateAllColumnsString};");
            }

            mySqlCommand.CommandText = stringBuilder.ToString();
            return mySqlCommand;
        }


        private MySqlCommand CreateDeleteCommand()
        {
            var mySqlCommand = new MySqlCommand();
            var stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"DELETE FROM {this.tableName.Quoted()} WHERE");
            stringBuilder.AppendLine($"{MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.GetPrimaryKeysColumns(), "", "@")};");

            mySqlCommand.CommandText = stringBuilder.ToString();
            return mySqlCommand;
        }

        private void SetUpdateRowParameters(DbCommand command)
        {
            DbParameter p;

            foreach (var column in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(column, "`").Unquoted().Normalized().ToString();

                p = command.CreateParameter();
                p.ParameterName = $"@{columnName}";
                p.DbType = column.GetDbType();
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }

            p = command.CreateParameter();
            p.ParameterName = "@row_count";
            p.DbType = DbType.Int32;
            command.Parameters.Add(p);

        }

        private void SetDeleteRowParameters(DbCommand command)
        {
            DbParameter p;

            foreach (var column in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var quotedColumn = ParserName.Parse(column, "`").Unquoted().Normalized().ToString();

                p = command.CreateParameter();
                p.ParameterName = $"@{quotedColumn}";
                p.DbType = column.GetDbType();
                p.SourceColumn = column.ColumnName;
                command.Parameters.Add(p);
            }

        }


    }
}
