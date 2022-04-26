using Dotmim.Sync.Builders;
using Dotmim.Sync.Manager;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.SqlServer.Builders;
using Dotmim.Sync.SqlServer.Manager;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
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
    /// This provider does not create any triggers or tracking tables in the SQLit client database
    /// If your client database is not readonly, any changes from server will overwrite the client changes
    /// </summary>

    public class SqliteSyncDownloadOnlyProvider : SqliteSyncProvider
    {
        public SqliteSyncDownloadOnlyProvider() { }
        public SqliteSyncDownloadOnlyProvider(string connectionString) : base(connectionString) { }
        public SqliteSyncDownloadOnlyProvider(SqliteConnectionStringBuilder builder) : base(builder) { }

        /// <summary>
        /// we still need the scope_info table. Nothing changes here
        /// </summary>
        public override DbScopeBuilder GetScopeBuilder(string scopeInfoTableName) => base.GetScopeBuilder(scopeInfoTableName);


        /// <summary>
        /// Get a specific adapter for a readonly sqlite database
        /// </summary>
        public override DbSyncAdapter GetSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
            => new SqliteDownloadOnlySyncAdapter(tableDescription, tableName, trackingTableName, setup, scopeName);

        /// <summary>
        /// Nothing changes here
        /// </summary>
        public override DbBuilder GetDatabaseBuilder() => base.GetDatabaseBuilder();

        /// <summary>
        /// Removing tracking tables & triggers since they are not needed here
        /// </summary>
        public override DbTableBuilder GetTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
            => new SqliteDownloadOnlyTableBuilder(tableDescription, tableName, trackingTableName, setup, scopeName);
    }

    /// <summary>
    /// On the table builder, just return null for tracking table and triggers creation
    /// </summary>
    public class SqliteDownloadOnlyTableBuilder : SqliteTableBuilder
    {
        public SqliteDownloadOnlyTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
            : base(tableDescription, tableName, trackingTableName, setup, scopeName) { }
        public override Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => Task.FromResult<DbCommand>(null);
        public override Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => Task.FromResult<DbCommand>(null);
    }


    /// <summary>
    /// On the adapter, change the update / delete sqlite command text. Return null for everything related to upload stuff
    /// </summary>
    public class SqliteDownloadOnlySyncAdapter : SqliteSyncAdapter
    {
        private SqliteObjectNames sqliteObjectNames;
        private ParserName tableName;

        public SqliteDownloadOnlySyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup, string scopeName) 
            : base(tableDescription, tableName, trackingName, setup, scopeName)
        {
            this.sqliteObjectNames = new SqliteObjectNames(tableDescription, tableName, trackingName, setup, scopeName);
            this.tableName = tableName;
        }

        /// <summary>
        /// return null for all no used commands
        /// </summary>
        public override (DbCommand, bool) GetCommand(DbCommandType nameType, SyncFilter filter)
        {
            DbCommand command;
            switch (nameType)
            {
                case DbCommandType.UpdateRow:
                    command = CreateUpdateCommand();
                    break;
                case DbCommandType.InsertRow:
                    command = CreateInitiliazeRowCommand();
                    break;
                case DbCommandType.DeleteRow:
                    command = CreateDeleteCommand();
                    break;
                case DbCommandType.DisableConstraints:
                    command = new SqliteCommand();
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.sqliteObjectNames.GetCommandName(DbCommandType.DisableConstraints, filter);
                    break;
                case DbCommandType.EnableConstraints:
                    command = new SqliteCommand();
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.sqliteObjectNames.GetCommandName(DbCommandType.EnableConstraints, filter);
                    break;
                case DbCommandType.Reset:
                    command = CreateResetCommand();
                    break;
                default:
                    return (null, false);
            }

            return (command, false);
        }

        /// <summary>
        /// Gets the init row command without adding rows in tracking table
        /// </summary>
        private DbCommand CreateInitiliazeRowCommand()
        {
            var stringBuilder = new StringBuilder();

            var columnToInserts = new StringBuilder();
            var valuesToInserts = new StringBuilder();
            string empty = string.Empty;
            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                var columnParameterName = ParserName.Parse(mutableColumn).Unquoted().Normalized().ToString();

                valuesToInserts.Append($"{empty}@{columnParameterName}");
                columnToInserts.Append($"{empty}{columnName}");
                empty = ", ";
            }

            stringBuilder.AppendLine($"INSERT OR IGNORE INTO {tableName.Quoted()} ({columnToInserts})");
            stringBuilder.AppendLine($"VALUES ({valuesToInserts});");

            var cmdtext = stringBuilder.ToString();
            return new SqliteCommand(cmdtext);

        }

        /// <summary>
        /// Gets the delete row command without adding rows in tracking table
        /// </summary>
        private DbCommand CreateDeleteCommand()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"DELETE FROM {tableName.Quoted()} ");
            stringBuilder.AppendLine($"WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, "")};");
            return new SqliteCommand(stringBuilder.ToString());
        }

        /// <summary>
        /// Gets the update row command without adding rows in tracking table, or compare timestamp
        /// </summary>
        private DbCommand CreateUpdateCommand()
        {
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();
            var stringBuilderParametersValues = new StringBuilder();
            string empty = string.Empty;

            // Generate Update command
            var stringBuilder = new StringBuilder();

            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                var columnParameterName = ParserName.Parse(mutableColumn).Unquoted().Normalized().ToString();

                stringBuilderParametersValues.Append($"{empty}@{columnParameterName} as {columnName}");
                stringBuilderArguments.Append($"{empty}{columnName}");
                stringBuilderParameters.Append($"{empty}[c].{columnName}");
                empty = "\n, ";
            }

            // create update statement without PK
            var emptyUpdate = string.Empty;
            var columnsToUpdate = false;
            var stringBuilderUpdateSet = new StringBuilder();
            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, false))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilderUpdateSet.Append($"{emptyUpdate}{columnName}=excluded.{columnName}");
                emptyUpdate = "\n, ";

                columnsToUpdate = true;
            }

            var primaryKeys = string.Join(",",
                this.TableDescription.PrimaryKeys.Select(name => ParserName.Parse(name).Quoted().ToString()));

            // add CTE
            stringBuilder.AppendLine($"WITH CHANGESET as (SELECT {stringBuilderParameters} ");
            stringBuilder.AppendLine($"FROM (SELECT {stringBuilderParametersValues}) as [c])");
            stringBuilder.AppendLine($"INSERT INTO {this.tableName.Quoted()}");
            stringBuilder.AppendLine($"({stringBuilderArguments})");
            stringBuilder.AppendLine($" SELECT * from CHANGESET WHERE TRUE");

            if (columnsToUpdate)
            {
                stringBuilder.AppendLine($" ON CONFLICT ({primaryKeys}) DO UPDATE SET ");
                stringBuilder.Append(stringBuilderUpdateSet.ToString()).AppendLine(";");
            }
            else
            {
                stringBuilder.AppendLine($" ON CONFLICT ({primaryKeys}) DO NOTHING; ");
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine();

            return new SqliteCommand(stringBuilder.ToString());
        }

        /// <summary>
        /// Gets the reset command without reseting the tracking table 
        /// </summary>
        private DbCommand CreateResetCommand()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"DELETE FROM {tableName.Quoted().ToString()};");
            return new SqliteCommand(stringBuilder.ToString());
        }
    }
}
