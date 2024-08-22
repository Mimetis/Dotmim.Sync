using Dotmim.Sync;
using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;
using Dotmim.Sync.Sqlite;
using Microsoft.Data.Sqlite;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace CustomProvider
{
    /// <summary>
    /// Download Only Provider for Sqlite.
    /// </summary>
    public class SqliteSyncDownloadOnlyProvider : SqliteSyncProvider
    {

        public SqliteSyncDownloadOnlyProvider(string filePath)
            : base(filePath)
        {
        }

        public override DbSyncAdapter GetSyncAdapter(SyncTable tableDescription, ScopeInfo scopeInfo)
            => new SqliteDownloadOnlySyncAdapter(tableDescription, scopeInfo, true);
    }

    public class SqliteDownloadOnlySyncAdapter : SqliteSyncAdapter
    {

        private SqliteDownloadOnlyTableBuilder sqliteDownloadOnlyTableBuilder;

        public SqliteDownloadOnlySyncAdapter(SyncTable tableDescription, ScopeInfo scopeInfo, bool disableSqlFiltersGeneration)
            : base(tableDescription, scopeInfo, disableSqlFiltersGeneration)
        {
            this.sqliteDownloadOnlyTableBuilder = new SqliteDownloadOnlyTableBuilder(tableDescription, scopeInfo, disableSqlFiltersGeneration);
        }

        /// <summary>
        /// Returns the table builder.
        /// </summary>
        public override DbTableBuilder GetTableBuilder() => this.sqliteDownloadOnlyTableBuilder;

        /// <summary>
        /// Overriding get command to replace with really simple and performant instructions.
        /// </summary>
        public override (DbCommand, bool) GetCommand(SyncContext context, DbCommandType commandType, SyncFilter filter = null)
        {
            var command = commandType switch
            {
                DbCommandType.UpdateRow or DbCommandType.UpdateRows => (this.CreateUpdateCommand(), false),
                DbCommandType.InsertRow or DbCommandType.InsertRows => (this.CreateInsertCommand(), false),
                DbCommandType.DeleteRow or DbCommandType.DeleteRows => (this.CreateDeleteCommand(), false),
                DbCommandType.DisableConstraints => base.GetCommand(context, DbCommandType.DisableConstraints),
                DbCommandType.EnableConstraints => base.GetCommand(context, DbCommandType.EnableConstraints),
                DbCommandType.Reset => (this.CreateResetCommand(), false),
                _ => (null, false),
            };

            return command;
        }

        /// <summary>
        /// Gets the init row command without adding rows in tracking table.
        /// </summary>
        private SqliteCommand CreateInsertCommand()
        {
            var stringBuilder = new StringBuilder();

            var columnToInserts = new StringBuilder();
            var valuesToInserts = new StringBuilder();
            string empty = string.Empty;
            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);

                valuesToInserts.Append($"{empty}@{columnParser.NormalizedShortName}");
                columnToInserts.Append($"{empty}{columnParser.QuotedShortName}");
                empty = ", ";
            }

            stringBuilder.AppendLine($"INSERT OR IGNORE INTO {this.SqliteObjectNames.TableQuotedShortName} ({columnToInserts})");
            stringBuilder.AppendLine($"VALUES ({valuesToInserts});");

            var cmdtext = stringBuilder.ToString();

            return new SqliteCommand(cmdtext);
        }

        /// <summary>
        /// Gets the delete row command without adding rows in tracking table.
        /// </summary>
        private SqliteCommand CreateDeleteCommand()
        {
            var stringBuilder = new StringBuilder();
            string separatorString = string.Empty;
            stringBuilder.AppendLine($"DELETE FROM {this.SqliteObjectNames.TableQuotedShortName} WHERE ");

            foreach (var column in this.TableDescription.GetPrimaryKeysColumns())
            {
                var objectParser = new ObjectParser(column.ColumnName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);

                stringBuilder.Append(separatorString);
                stringBuilder.Append(objectParser.QuotedShortName);
                stringBuilder.Append(" = ");
                stringBuilder.Append($"@{objectParser.NormalizedShortName}");
                separatorString = " AND ";
            }

            return new SqliteCommand(stringBuilder.ToString());
        }

        /// <summary>
        /// Gets the update row command without adding rows in tracking table, or compare timestamp.
        /// </summary>
        private SqliteCommand CreateUpdateCommand()
        {
            var stringBuilder = new StringBuilder();

            var columnToInserts = new StringBuilder();
            var valuesToInserts = new StringBuilder();
            string empty = string.Empty;
            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);

                valuesToInserts.Append($"{empty}@{columnParser.NormalizedShortName}");
                columnToInserts.Append($"{empty}{columnParser.QuotedShortName}");
                empty = ", ";
            }

            stringBuilder.AppendLine($"INSERT OR REPLACE INTO {this.SqliteObjectNames.TableQuotedShortName} ({columnToInserts})");
            stringBuilder.AppendLine($"VALUES ({valuesToInserts});");

            var cmdtext = stringBuilder.ToString();
            return new SqliteCommand(cmdtext);
        }

        /// <summary>
        /// Gets the reset command without reseting the tracking table.
        /// </summary>
        private SqliteCommand CreateResetCommand()
            => new SqliteCommand($"DELETE FROM {this.SqliteObjectNames.TableQuotedShortName};");
    }

    /// <summary>
    /// Sqlite table builder without tracking tables and triggers.
    /// </summary>
    public class SqliteDownloadOnlyTableBuilder : SqliteTableBuilder
    {
        public SqliteDownloadOnlyTableBuilder(SyncTable tableDescription, ScopeInfo scopeInfo, bool disableSqlFiltersGeneration)
            : base(tableDescription, scopeInfo, disableSqlFiltersGeneration)
        {
        }

        public override Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => Task.FromResult<DbCommand>(null);

        public override Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => Task.FromResult<DbCommand>(null);
    }
}