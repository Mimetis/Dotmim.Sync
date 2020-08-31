using Dotmim.Sync.Builders;
using System;
using System.Text;

using System.Data.Common;

using System.Data;
using Microsoft.Data.Sqlite;

using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dotmim.Sync.Sqlite
{
    public class SqliteBuilderTrackingTable : IDbBuilderTrackingTableHelper
    {
        private ParserName tableName;
        private ParserName trackingName;
        private SyncTable tableDescription;
        private SyncSetup setup;
        private SqliteDbMetadata sqliteDbMetadata;

        public SqliteBuilderTrackingTable(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
        {
            this.tableDescription = tableDescription;
            this.setup = setup;
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.sqliteDbMetadata = new SqliteDbMetadata();
        }


        public Task CreateIndexAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;

        public Task CreatePkAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;

        public async Task CreateTableAsync(DbConnection connection, DbTransaction transaction)
        {
            using (var command = new SqliteCommand(this.CreateTableCommandText(), (SqliteConnection)connection, (SqliteTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);

            }
        }

        public string CreateTableCommandText()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE TABLE {trackingName.Quoted().ToString()} (");

            // Adding the primary key
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var quotedColumnName = ParserName.Parse(pkColumn).Quoted().ToString();

                var columnTypeString = this.sqliteDbMetadata.TryGetOwnerDbTypeString(pkColumn.OriginalDbType, pkColumn.GetDbType(), false, false, pkColumn.MaxLength, this.tableDescription.OriginalProvider, SqliteSyncProvider.ProviderType);
                var columnPrecisionString = this.sqliteDbMetadata.TryGetOwnerDbTypePrecision(pkColumn.OriginalDbType, pkColumn.GetDbType(), false, false, pkColumn.MaxLength, pkColumn.Precision, pkColumn.Scale, this.tableDescription.OriginalProvider, SqliteSyncProvider.ProviderType);
                var quotedColumnType = ParserName.Parse(columnTypeString).Quoted().ToString();
                quotedColumnType += columnPrecisionString;

                stringBuilder.AppendLine($"{quotedColumnName} {quotedColumnType} NOT NULL COLLATE NOCASE, ");
            }

            // adding the tracking columns
            stringBuilder.AppendLine($"[update_scope_id] [text] NULL COLLATE NOCASE, ");
            stringBuilder.AppendLine($"[timestamp] [integer] NULL, ");
            stringBuilder.AppendLine($"[sync_row_is_tombstone] [integer] NOT NULL default(0), ");
            stringBuilder.AppendLine($"[last_change_datetime] [datetime] NULL, ");

            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < this.tableDescription.PrimaryKeys.Count; i++)
            {
                var pkColumn = this.tableDescription.PrimaryKeys[i];
                var quotedColumnName = ParserName.Parse(pkColumn).Quoted().ToString();

                stringBuilder.Append(quotedColumnName);

                if (i < this.tableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.Append(")");


            stringBuilder.Append(");");

            stringBuilder.AppendLine($"CREATE INDEX [{trackingName.Schema().Unquoted().Normalized().ToString()}_timestamp_index] ON {trackingName.Schema().Quoted().ToString()} (");
            stringBuilder.AppendLine($"\t [timestamp] ASC");
            stringBuilder.AppendLine($"\t,[update_scope_id] ASC");
            stringBuilder.AppendLine($"\t,[sync_row_is_tombstone] ASC");
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.AppendLine($"\t,{columnName} ASC");
            }
            stringBuilder.Append(");");
            return stringBuilder.ToString();
        }

        public async Task<bool> NeedToCreateTrackingTableAsync(DbConnection connection, DbTransaction transaction)
            => !await SqliteManagementUtils.TableExistsAsync((SqliteConnection)connection, (SqliteTransaction)transaction, trackingName).ConfigureAwait(false);


        public async Task DropTableAsync(DbConnection connection, DbTransaction transaction)
        {
            using (var command = new SqliteCommand($"DROP TABLE IF EXISTS {trackingName.Quoted().ToString()}", (SqliteConnection)connection, (SqliteTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        public async Task RenameTableAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction)
        {
            var tableNameString = this.trackingName.Quoted().ToString();
            var oldTableNameString = oldTableName.Quoted().ToString();

            var commandText = $"ALTER TABLE {oldTableNameString} RENAME TO {tableNameString};";

            using (var command = new SqliteCommand(commandText, (SqliteConnection)connection, (SqliteTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }
    }
}
