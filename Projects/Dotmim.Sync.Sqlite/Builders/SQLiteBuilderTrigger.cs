﻿using Dotmim.Sync.Builders;
using System;
using System.Text;

using System.Data.Common;
using System.Linq;

using System.Data;
using Microsoft.Data.Sqlite;

using System.Diagnostics;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Dotmim.Sync.Sqlite
{
    public class SqliteBuilderTrigger : IDbBuilderTriggerHelper
    {
        private ParserName tableName;
        private ParserName trackingName;
        private SyncTable tableDescription;
        private SyncSetup setup;
        private SqliteObjectNames sqliteObjectNames;
        public SqliteBuilderTrigger(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
        {
            this.tableDescription = tableDescription;
            this.setup = setup;
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.sqliteObjectNames = new SqliteObjectNames(this.tableDescription, tableName, trackingName, this.setup);
        }

        private string DeleteTriggerBodyText()
        {
            var stringBuilder = new StringBuilder();
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();
            string argComma = string.Empty;
            string argAnd = string.Empty;

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("BEGIN");

            stringBuilder.AppendLine($"\tINSERT OR REPLACE INTO {trackingName.Quoted().ToString()} (");
            foreach (var mutableColumn in this.tableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();

                stringBuilderArguments.AppendLine($"\t\t{argComma}{columnName}");
                stringBuilderArguments2.AppendLine($"\t\t{argComma}old.{columnName}");
                stringPkAreNull.Append($"{argAnd}{trackingName.Quoted().ToString()}.{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine("\t\t,[update_scope_id]");
            stringBuilder.AppendLine("\t\t,[timestamp]");
            stringBuilder.AppendLine("\t\t,[sync_row_is_tombstone]");
            stringBuilder.AppendLine("\t\t,[last_change_datetime]");

            stringBuilder.AppendLine("\t) ");
            stringBuilder.AppendLine("\tVALUES (");
            stringBuilder.Append(stringBuilderArguments2.ToString());
            stringBuilder.AppendLine("\t\t,NULL");
            stringBuilder.AppendLine($"\t\t,{SqliteObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t\t,1");
            stringBuilder.AppendLine("\t\t,datetime('now')");
            stringBuilder.AppendLine("\t);");
            stringBuilder.AppendLine("END;");
            return stringBuilder.ToString();
        }
        public async Task CreateDeleteTriggerAsync(DbConnection connection, DbTransaction transaction)
        {
            var delTriggerName = this.sqliteObjectNames.GetCommandName(DbCommandType.DeleteTrigger);
            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER IF NOT EXISTS {delTriggerName} AFTER DELETE ON {tableName.Quoted().ToString()} ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.DeleteTriggerBodyText());

            using (var command = new SqliteCommand(createTrigger.ToString(), (SqliteConnection)connection, (SqliteTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        public Task AlterDeleteTriggerAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;

        private string InsertTriggerBodyText()
        {
            var stringBuilder = new StringBuilder();
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();
            string argComma = string.Empty;
            string argAnd = string.Empty;

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("BEGIN");
            stringBuilder.AppendLine("-- If row was deleted before, it already exists, so just make an update");

            stringBuilder.AppendLine($"\tINSERT OR REPLACE INTO {trackingName.Quoted().ToString()} (");
            foreach (var mutableColumn in this.tableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();

                stringBuilderArguments.AppendLine($"\t\t{argComma}{columnName}");
                stringBuilderArguments2.AppendLine($"\t\t{argComma}new.{columnName}");
                stringPkAreNull.Append($"{argAnd}{trackingName.Quoted().ToString()}.{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine("\t\t,[update_scope_id]");
            stringBuilder.AppendLine("\t\t,[timestamp]");
            stringBuilder.AppendLine("\t\t,[sync_row_is_tombstone]");
            stringBuilder.AppendLine("\t\t,[last_change_datetime]");

            stringBuilder.AppendLine("\t) ");
            stringBuilder.AppendLine("\tVALUES (");
            stringBuilder.Append(stringBuilderArguments2.ToString());
            stringBuilder.AppendLine("\t\t,NULL");
            stringBuilder.AppendLine($"\t\t,{SqliteObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t\t,0");
            stringBuilder.AppendLine("\t\t,datetime('now')");
            stringBuilder.AppendLine("\t);");
            stringBuilder.AppendLine("END;");
            return stringBuilder.ToString();
        }

        public async Task CreateInsertTriggerAsync(DbConnection connection, DbTransaction transaction)
        {
            var insTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.InsertTrigger), tableName.Unquoted().ToString());
            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER IF NOT EXISTS {insTriggerName} AFTER INSERT ON {tableName.Quoted().ToString()} ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            using (var command = new SqliteCommand(createTrigger.ToString(), (SqliteConnection)connection, (SqliteTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        public Task AlterInsertTriggerAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;

        private string UpdateTriggerBodyText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"Begin ");
            stringBuilder.AppendLine($"\tUPDATE {trackingName.Quoted().ToString()} ");
            stringBuilder.AppendLine("\tSET [update_scope_id] = NULL -- scope id is always NULL when update is made locally");
            stringBuilder.AppendLine($"\t\t,[timestamp] = {SqliteObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t\t,[last_change_datetime] = datetime('now')");

            stringBuilder.Append($"\tWhere ");
            stringBuilder.Append(SqliteManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, trackingName.Quoted().ToString(), "new"));


            if (this.tableDescription.GetMutableColumns().Count() > 0)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine("\t AND (");
                string or = "    ";
                foreach (var column in this.tableDescription.GetMutableColumns())
                {
                    var quotedColumn = ParserName.Parse(column).Quoted().ToString();

                    stringBuilder.Append("\t");
                    stringBuilder.Append(or);
                    stringBuilder.Append("IFNULL(");
                    stringBuilder.Append("NULLIF(");
                    stringBuilder.Append("[old].");
                    stringBuilder.Append(quotedColumn);
                    stringBuilder.Append(", ");
                    stringBuilder.Append("[new].");
                    stringBuilder.Append(quotedColumn);
                    stringBuilder.Append(")");
                    stringBuilder.Append(", ");
                    stringBuilder.Append("NULLIF(");
                    stringBuilder.Append("[new].");
                    stringBuilder.Append(quotedColumn);
                    stringBuilder.Append(", ");
                    stringBuilder.Append("[old].");
                    stringBuilder.Append(quotedColumn);
                    stringBuilder.Append(")");
                    stringBuilder.AppendLine(") IS NOT NULL");

                    or = " OR ";
                }
                stringBuilder.AppendLine("\t ) ");
            }

            stringBuilder.AppendLine($"; ");


            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();
            string argComma = string.Empty;
            string argAnd = string.Empty;

            stringBuilder.AppendLine($"\tINSERT OR IGNORE INTO {trackingName.Quoted().ToString()} (");
            foreach (var mutableColumn in this.tableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();

                stringBuilderArguments.AppendLine($"\t\t{argComma}{columnName}");
                stringBuilderArguments2.AppendLine($"\t\t{argComma}new.{columnName}");
                stringPkAreNull.Append($"{argAnd}{trackingName.Quoted().ToString()}.{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine("\t\t,[update_scope_id]");
            stringBuilder.AppendLine("\t\t,[timestamp]");
            stringBuilder.AppendLine("\t\t,[sync_row_is_tombstone]");
            stringBuilder.AppendLine("\t\t,[last_change_datetime]");

            stringBuilder.AppendLine("\t) ");
            stringBuilder.AppendLine("\tVALUES (");
            stringBuilder.Append(stringBuilderArguments2.ToString());
            stringBuilder.AppendLine("\t\t,NULL");
            stringBuilder.AppendLine($"\t\t,{SqliteObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t\t,0");
            stringBuilder.AppendLine("\t\t,datetime('now')");
            stringBuilder.AppendLine("\t);");

            stringBuilder.AppendLine($"End; ");
            return stringBuilder.ToString();
        }

        public async Task CreateUpdateTriggerAsync(DbConnection connection, DbTransaction transaction)
        {
            var updTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.UpdateTrigger), tableName.Unquoted().ToString());
            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER IF NOT EXISTS {updTriggerName} AFTER UPDATE ON {tableName.Quoted().ToString()} ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.UpdateTriggerBodyText());

            using (var command = new SqliteCommand(createTrigger.ToString(), (SqliteConnection)connection, (SqliteTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }

        }
        public Task AlterUpdateTriggerAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;

        public async Task<bool> NeedToCreateTriggerAsync(DbTriggerType type, DbConnection connection, DbTransaction transaction)
        {
            var updTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.UpdateTrigger), tableName.Unquoted().ToString());
            var delTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.DeleteTrigger), tableName.Unquoted().ToString());
            var insTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.InsertTrigger), tableName.Unquoted().ToString());

            string triggerName = string.Empty;
            switch (type)
            {
                case DbTriggerType.Insert:
                    {
                        triggerName = insTriggerName;
                        break;
                    }
                case DbTriggerType.Update:
                    {
                        triggerName = updTriggerName;
                        break;
                    }
                case DbTriggerType.Delete:
                    {
                        triggerName = delTriggerName;
                        break;
                    }
            }

            return !await SqliteManagementUtils.TriggerExistsAsync((SqliteConnection)connection, (SqliteTransaction)transaction, triggerName).ConfigureAwait(false);
        }


        public Task DropInsertTriggerAsync(DbConnection connection, DbTransaction transaction) => this.DropTriggerAsync(DbCommandType.InsertTrigger, connection, transaction);
        public Task DropUpdateTriggerAsync(DbConnection connection, DbTransaction transaction) => this.DropTriggerAsync(DbCommandType.UpdateTrigger, connection, transaction);
        public Task DropDeleteTriggerAsync(DbConnection connection, DbTransaction transaction) => this.DropTriggerAsync(DbCommandType.DeleteTrigger, connection, transaction);

        private async Task DropTriggerAsync(DbCommandType triggerType, DbConnection connection, DbTransaction transaction)
        {
            var triggerName = string.Format(this.sqliteObjectNames.GetCommandName(triggerType), tableName.Unquoted().ToString());
            var dropTrigger = $"DROP TRIGGER IF EXISTS {triggerName}";

            using (var command = new SqliteCommand(dropTrigger, (SqliteConnection)connection, (SqliteTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }
    }
}
