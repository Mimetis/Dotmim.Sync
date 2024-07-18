using Dotmim.Sync.Builders;
using Dotmim.Sync.Sqlite;
using Microsoft.Data.Sqlite;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Tests
{
    public class SqliteSyncDownloadOnlyProvider : SqliteSyncProvider
    {
        public SqliteSyncDownloadOnlyProvider() { }
        public SqliteSyncDownloadOnlyProvider(string connectionString) : base(connectionString) { }
        public SqliteSyncDownloadOnlyProvider(SqliteConnectionStringBuilder builder) : base(builder) { }


        public override string GetShortProviderTypeName() => typeof(SqliteSyncDownloadOnlyProvider).Name;

        /// <summary>
        /// Get a specific adapter for a readonly sqlite database
        /// </summary>
        public override DbSyncAdapter GetSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
            => new SqliteDownloadOnlySyncAdapter(tableDescription, tableName, trackingTableName, setup, scopeName, DisableSqlFiltersGeneration);


        /// <summary>
        /// Removing tracking tables & triggers since they are not needed here
        /// </summary>
        public override DbTableBuilder GetTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
            => new SqliteDownloadOnlyTableBuilder(tableDescription, tableName, trackingTableName, setup, scopeName, this.DisableSqlFiltersGeneration);
    }


    /// <summary>
    /// Sqlite table builder without tracking tables and triggers
    /// </summary>
    public class SqliteDownloadOnlyTableBuilder : SqliteTableBuilder
    {
        public SqliteDownloadOnlyTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName, bool disableSqlFiltersGeneration)
            : base(tableDescription, tableName, trackingTableName, setup, scopeName, disableSqlFiltersGeneration) { }
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

        public SqliteDownloadOnlySyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup, string scopeName, bool disableSqlFiltersGeneration)
            : base(tableDescription, tableName, trackingName, setup, scopeName, disableSqlFiltersGeneration)
        {
            this.sqliteObjectNames = new SqliteObjectNames(tableDescription, tableName, trackingName, setup, scopeName, disableSqlFiltersGeneration);
            this.tableName = tableName;
        }

        

        /// <summary>
        /// return null for all no used commands
        /// </summary>
        public override (DbCommand, bool) GetCommand(SyncContext context, DbCommandType nameType, SyncFilter filter)
        {
            var command = new SqliteCommand();
            switch (nameType)
            {
                case DbCommandType.UpdateRow:
                case DbCommandType.UpdateRows:
                    return (CreateUpdateCommand(), false);
                case DbCommandType.InsertRow:
                case DbCommandType.InsertRows:
                    return (CreateInsertRowCommand(), false);
                case DbCommandType.DeleteRow:
                case DbCommandType.DeleteRows:
                    return (CreateDeleteCommand(), false);
                case DbCommandType.DisableConstraints:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.sqliteObjectNames.GetCommandName(DbCommandType.DisableConstraints, filter);
                    break;
                case DbCommandType.EnableConstraints:
                    command.CommandType = CommandType.Text;
                    command.CommandText = this.sqliteObjectNames.GetCommandName(DbCommandType.EnableConstraints, filter);
                    break;
                case DbCommandType.Reset:
                    return (CreateResetCommand(), false);
                default:
                    return (default, default);
            }

            return (command, false);
        }

        /// <summary>
        /// Gets the init row command without adding rows in tracking table
        /// </summary>
        private DbCommand CreateInsertRowCommand()
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

            stringBuilder.AppendLine($"INSERT OR REPLACE INTO {tableName.Quoted()} ({columnToInserts})");
            stringBuilder.AppendLine($"VALUES ({valuesToInserts});");

            var cmdtext = stringBuilder.ToString();
            return new SqliteCommand(cmdtext);
        }

        /// <summary>
        /// Gets the reset command without reseting the tracking table 
        /// </summary>
        private DbCommand CreateResetCommand()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"DELETE FROM {tableName.Quoted()};");
            return new SqliteCommand(stringBuilder.ToString());
        }
    }

}
