using Dotmim.Sync.Builders;
using Dotmim.Sync.Manager;
using Dotmim.Sync.MariaDB;
using Dotmim.Sync.MariaDB.Builders;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.SqlServer.Builders;
using Dotmim.Sync.SqlServer.Manager;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.SampleConsole
{

    public class MariaDBSyncProvider2 : MariaDBSyncProvider
    {
        public MariaDBSyncProvider2() { }
        public MariaDBSyncProvider2(string connectionString) : base(connectionString) { }
        public MariaDBSyncProvider2(MySqlConnectionStringBuilder builder) : base(builder) { }


        public override DbBuilder GetDatabaseBuilder() => new MariaDBDownloadOnlyBuilder();
    }

    public class MariaDBDownloadOnlyBuilder : MySqlBuilder
    {
        //public override Task EnsureDatabaseAsync(DbConnection connection, DbTransaction transaction = null)
        //{
        //    return Task.CompletedTask;
        //}
    }

    /// <summary>
    /// Use this provider if your client database does not need to upload any data to the server.
    /// This provider does not create any triggers / tracking tables and only 3 stored proc / tables
    /// If your client database is not readonly, any changes from server will overwrite client changes
    /// </summary>
    public class MariaDBSyncDownloadOnlyProvider : MariaDBSyncProvider
    {
        public MariaDBSyncDownloadOnlyProvider() { }
        public MariaDBSyncDownloadOnlyProvider(string connectionString) : base(connectionString) { }
        public MariaDBSyncDownloadOnlyProvider(MySqlConnectionStringBuilder builder) : base(builder) { }

        public override DbSyncAdapter GetSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup)
            => new MariaDBDownloadOnlySyncAdapter(tableDescription, tableName, trackingTableName, setup, this.BulkBatchMaxLinesCount);

        public override DbTableBuilder GetTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup)
            => new MariaDBDownloadOnlyTableBuilder(tableDescription, tableName, trackingTableName, setup);

        // Max number of lines in batch bulk init operation
        public override int BulkBatchMaxLinesCount => 100;
    }

    /// <summary>
    /// Table builder builds table, tracking tables, triggers, stored proc, types
    /// </summary>
    public class MariaDBDownloadOnlyTableBuilder : MySqlTableBuilder
    {
        public MariaDBDownloadOnlyTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup)
            : base(tableDescription, tableName, trackingTableName, setup) { }

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
    public class MariaDBDownloadOnlySyncAdapter : MySqlSyncAdapter
    {
        private ParserName tableName;
        private readonly int bulkBatchMaxLinesCount;

        public MariaDBDownloadOnlySyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup, int bulkBatchMaxLinesCount)
            : base(tableDescription, tableName, trackingName, setup)
        {
            this.tableName = tableName;
            this.bulkBatchMaxLinesCount = bulkBatchMaxLinesCount;
        }

        /// <summary>
        /// Returning null for all non used commands (from case default)
        /// </summary>
        public override (DbCommand, bool) GetCommand(DbCommandType nameType, SyncFilter filter)
        {
            var command = new MySqlCommand();
            var isBatch = false;
            switch (nameType)
            {
                case DbCommandType.UpdateRows:
                case DbCommandType.UpdateRow:
                    command = CreateUpdateCommand();
                    break;
                case DbCommandType.InsertRows:
                    command = CreateBulkInitializeCommand(this.bulkBatchMaxLinesCount);
                    isBatch = true;
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
                default:
                    return (null, false);
            }
            return (command, isBatch);
        }

        public override async Task ExecuteBatchCommandAsync(DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> arrayItems, SyncTable schemaChangesTable, SyncTable failedRows, long? lastTimestamp, DbConnection connection, DbTransaction transaction = null)
        {
            using var ms = new MemoryStream();
            using var sw = new StreamWriter(ms);
            var shouldDispose = false;
            MySqlCommand batchCommand;

            var lstItems = arrayItems.ToList();

            if (lstItems.Count == bulkBatchMaxLinesCount)
            {
                batchCommand = cmd as MySqlCommand;
                batchCommand.Parameters.Clear();
            }
            else
            {
                batchCommand = CreateBulkInitializeCommand(lstItems.Count);
                batchCommand.Connection = connection as MySqlConnection;
                batchCommand.Transaction = transaction as MySqlTransaction;
                shouldDispose = true;
            }

            for (int i = 0; i < lstItems.Count; i++)
            {
                var row = lstItems[i];

                int columnIndex = 0;
                foreach (var column in schemaChangesTable.Columns)
                {
                    var parameterColumnName = ParserName.Parse(column, "`").Unquoted().Normalized().ToString();
                    var parameterName = $"@p{i}_{parameterColumnName}";
                    batchCommand.Parameters.AddWithValue(parameterName, row[columnIndex]);
                    columnIndex++;
                }
            }

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                using var dataReader = await batchCommand.ExecuteReaderAsync().ConfigureAwait(false);

                dataReader.Close();

                if (shouldDispose)
                    batchCommand.Dispose();

            }
            catch (DbException ex)
            {
                Debug.WriteLine(ex.Message);
                throw;
            }
            finally
            {

                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();
            }
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

            var setUpdateAllColumnsString = new StringBuilder();
            var allColumnsString = new StringBuilder();
            var allColumnsValuesString = new StringBuilder();

            string empty = string.Empty;
            foreach (var mutableColumn in this.TableDescription.GetMutableColumns())
            {
                var mutableColumnName = ParserName.Parse(mutableColumn, "`").Quoted().ToString();
                var parameterColumnName = ParserName.Parse(mutableColumn, "`").Unquoted().Normalized().ToString();

                allColumnsString.Append($"{empty}{mutableColumnName}");
                allColumnsValuesString.Append($"{empty}@{parameterColumnName}");

                empty = ", ";
            }
            empty = string.Empty;
            foreach (var mutableColumn in this.TableDescription.GetMutableColumns())
            {
                var mutableColumnName = ParserName.Parse(mutableColumn, "`").Quoted().ToString();
                var parameterColumnName = ParserName.Parse(mutableColumn, "`").Unquoted().Normalized().ToString();
                setUpdateAllColumnsString.Append($"{empty}{mutableColumnName}=@{parameterColumnName}");
                empty = ", ";
            }

            stringBuilder.AppendLine($"INSERT IGNORE INTO {tableName.Quoted()} ");
            stringBuilder.AppendLine($"({allColumnsString})");
            stringBuilder.AppendLine($"VALUES ({allColumnsValuesString})");
            if (hasMutableColumns)
            {
                stringBuilder.AppendLine($"ON DUPLICATE KEY");
                stringBuilder.AppendLine($"UPDATE {setUpdateAllColumnsString};");
            }

            mySqlCommand.CommandText = stringBuilder.ToString();
            return mySqlCommand;
        }

        private MySqlCommand CreateBulkInitializeCommand(int nbLines = 1)
        {
            var mySqlCommand = new MySqlCommand();
            var stringBuilder = new StringBuilder();

            var allColumnsString = new StringBuilder();

            string empty = string.Empty;
            foreach (var mutableColumn in this.TableDescription.GetMutableColumns())
            {
                var mutableColumnName = ParserName.Parse(mutableColumn, "`").Quoted().ToString();
                allColumnsString.Append($"{empty}{mutableColumnName}");
                empty = ", ";
            }

            stringBuilder.AppendLine($"INSERT IGNORE INTO {tableName.Quoted()} ");
            stringBuilder.AppendLine($"({allColumnsString})");
            stringBuilder.AppendLine("VALUES ");

            string commaValues = " ";
            for (int i = 0; i < nbLines; i++)
            {
                stringBuilder.Append($"{commaValues}(");
                empty = "";
                foreach (var mutableColumn in this.TableDescription.GetMutableColumns())
                {
                    var parameterColumnName = ParserName.Parse(mutableColumn, "`").Unquoted().Normalized().ToString();
                    stringBuilder.Append($"{empty}@p{i}_{parameterColumnName}");
                    empty = ", ";
                }
                stringBuilder.AppendLine(")");
                commaValues = ",";
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
