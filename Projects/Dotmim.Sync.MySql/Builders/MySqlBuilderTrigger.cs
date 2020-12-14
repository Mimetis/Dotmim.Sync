using Dotmim.Sync.Builders;
using MySql.Data.MySqlClient;
using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.MySql
{
    public class MySqlBuilderTrigger
    {
        private ParserName tableName;
        private ParserName trackingName;
        private SyncTable tableDescription;
        private SyncSetup setup;
        private MySqlObjectNames mySqlObjectNames;

        public MySqlBuilderTrigger(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
        {
            this.tableDescription = tableDescription;
            this.setup = setup;
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.mySqlObjectNames = new MySqlObjectNames(this.tableDescription, tableName, trackingName, this.setup);
        }

        public MySqlBuilderTrigger()
        {
        }

        private string DeleteTriggerBodyText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("BEGIN");

            stringBuilder.AppendLine($"\tINSERT INTO {this.trackingName.Quoted().ToString()} (");

            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderArguments2 = new StringBuilder();
            StringBuilder stringPkAreNull = new StringBuilder();

            string argComma = string.Empty;
            string argAnd = string.Empty;
            foreach (var mutableColumn in this.tableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn, "`").Quoted().ToString();

                stringBuilderArguments.AppendLine($"\t\t{argComma}{columnName}");
                stringBuilderArguments2.AppendLine($"\t\t{argComma}old.{columnName}");
                stringPkAreNull.Append($"{argAnd}{trackingName.Quoted().ToString()}.{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine("\t\t,`update_scope_id`");
            stringBuilder.AppendLine("\t\t,`timestamp`");
            stringBuilder.AppendLine("\t\t,`sync_row_is_tombstone`");
            stringBuilder.AppendLine("\t\t,`last_change_datetime`");

            var filterColumnsString = new StringBuilder();
            var filterColumnsString2 = new StringBuilder();
            var filterColumnsString3 = new StringBuilder();

            stringBuilder.AppendLine("\t) ");
            stringBuilder.AppendLine("\tVALUES (");
            stringBuilder.Append(stringBuilderArguments2.ToString());
            stringBuilder.AppendLine("\t\t,NULL");
            stringBuilder.AppendLine($"\t\t,{MySqlObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t\t,1");
            stringBuilder.AppendLine("\t\t,utc_timestamp()");


            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine("ON DUPLICATE KEY UPDATE");
            stringBuilder.AppendLine("\t`update_scope_id` = NULL, ");
            stringBuilder.AppendLine("\t`sync_row_is_tombstone` = 1, ");
            stringBuilder.AppendLine($"\t`timestamp` = {MySqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine("\t`last_change_datetime` = utc_timestamp()");

            stringBuilder.Append(";");
            stringBuilder.AppendLine("END");
            return stringBuilder.ToString();
        }
        public DbCommand CreateDeleteTriggerCommand(DbConnection connection, DbTransaction transaction)
        {
            var triggerName = this.mySqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger).name;
            
            StringBuilder createTrigger = new StringBuilder();
            createTrigger.AppendLine($"DROP TRIGGER IF EXISTS {triggerName};");
            createTrigger.AppendLine($"CREATE TRIGGER {triggerName} AFTER DELETE ON {tableName.Quoted().ToString()} FOR EACH ROW ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.DeleteTriggerBodyText());

            var command = new MySqlCommand(createTrigger.ToString(), (MySqlConnection)connection, (MySqlTransaction)transaction);
            return command;
        }

        private string InsertTriggerBodyText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- If row was deleted before, it already exists, so just make an update");
            stringBuilder.AppendLine("BEGIN");

            stringBuilder.AppendLine($"\tINSERT INTO {trackingName.Quoted().ToString()} (");

            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();

            string argComma = string.Empty;
            string argAnd = string.Empty;
            foreach (var mutableColumn in this.tableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn, "`").Quoted().ToString();

                stringBuilderArguments.AppendLine($"\t\t{argComma}{columnName}");
                stringBuilderArguments2.AppendLine($"\t\t{argComma}new.{columnName}");
                stringPkAreNull.Append($"{argAnd}{trackingName.Quoted().ToString()}.{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine("\t\t,`update_scope_id`");
            stringBuilder.AppendLine("\t\t,`timestamp`");
            stringBuilder.AppendLine("\t\t,`sync_row_is_tombstone`");
            stringBuilder.AppendLine("\t\t,`last_change_datetime`");

            var filterColumnsString = new StringBuilder();
            var filterColumnsString2 = new StringBuilder();
            var filterColumnsString3 = new StringBuilder();

            stringBuilder.AppendLine("\t) ");
            stringBuilder.AppendLine("\tVALUES (");
            stringBuilder.Append(stringBuilderArguments2.ToString());
            stringBuilder.AppendLine("\t\t,NULL");
            stringBuilder.AppendLine($"\t\t,{MySqlObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t\t,0");
            stringBuilder.AppendLine("\t\t,utc_timestamp()");


            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine("ON DUPLICATE KEY UPDATE");
            stringBuilder.AppendLine("\t`update_scope_id` = NULL, ");
            stringBuilder.AppendLine("\t`sync_row_is_tombstone` = 0, ");
            stringBuilder.AppendLine($"\t`timestamp` = {MySqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine("\t`last_change_datetime` = utc_timestamp()");

            stringBuilder.Append(";");
            stringBuilder.AppendLine("END");
            return stringBuilder.ToString();
        }

        public DbCommand CreateInsertTriggerCommand(DbConnection connection, DbTransaction transaction)
        {
            var insTriggerName = string.Format(this.mySqlObjectNames.GetCommandName(DbCommandType.InsertTrigger).name, tableName.Unquoted().Normalized().ToString());

            StringBuilder createTrigger = new StringBuilder();
            createTrigger.AppendLine($"DROP TRIGGER IF EXISTS {insTriggerName}; ");
            createTrigger.AppendLine($"CREATE TRIGGER {insTriggerName} AFTER INSERT ON {tableName.Quoted().ToString()} FOR EACH ROW ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            return new MySqlCommand(createTrigger.ToString(), (MySqlConnection)connection, (MySqlTransaction)transaction);
        }
        private string UpdateTriggerBodyText()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"Begin ");
            stringBuilder.AppendLine($"\tUPDATE {trackingName.Quoted().ToString()} ");
            stringBuilder.AppendLine("\tSET `update_scope_id` = NULL ");
            stringBuilder.AppendLine($"\t\t,`timestamp` = {MySqlObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t\t,`last_change_datetime` = utc_timestamp()");

            stringBuilder.Append($"\tWhere ");
            stringBuilder.Append(MySqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.GetPrimaryKeysColumns(), trackingName.Quoted().ToString(), "new"));

            if (this.tableDescription.GetMutableColumns().Count() > 0)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine("\t AND (");
                string or = "    ";
                foreach (var column in this.tableDescription.GetMutableColumns())
                {
                    var quotedColumn = ParserName.Parse(column, "`").Quoted().ToString();

                    stringBuilder.Append("\t");
                    stringBuilder.Append(or);
                    stringBuilder.Append("IFNULL(");
                    stringBuilder.Append("NULLIF(");
                    stringBuilder.Append("`old`.");
                    stringBuilder.Append(quotedColumn);
                    stringBuilder.Append(", ");
                    stringBuilder.Append("`new`.");
                    stringBuilder.Append(quotedColumn);
                    stringBuilder.Append(")");
                    stringBuilder.Append(", ");
                    stringBuilder.Append("NULLIF(");
                    stringBuilder.Append("`new`.");
                    stringBuilder.Append(quotedColumn);
                    stringBuilder.Append(", ");
                    stringBuilder.Append("`old`.");
                    stringBuilder.Append(quotedColumn);
                    stringBuilder.Append(")");
                    stringBuilder.AppendLine(") IS NOT NULL");

                    or = " OR ";
                }
                stringBuilder.AppendLine("\t ) ");
            }
            stringBuilder.AppendLine($"; ");

            stringBuilder.AppendLine("IF (SELECT ROW_COUNT() = 0) THEN ");

            stringBuilder.AppendLine($"\tINSERT INTO {trackingName.Quoted().ToString()} (");

            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderArguments2 = new StringBuilder();
            StringBuilder stringPkAreNull = new StringBuilder();

            string argComma = string.Empty;
            string argAnd = string.Empty;
            foreach (var mutableColumn in this.tableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn, "`").Quoted().ToString();

                stringBuilderArguments.AppendLine($"\t\t{argComma}{columnName}");
                stringBuilderArguments2.AppendLine($"\t\t{argComma}new.{columnName}");
                stringPkAreNull.Append($"{argAnd}{trackingName.Quoted().ToString()}.{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine("\t\t,`update_scope_id`");
            stringBuilder.AppendLine("\t\t,`timestamp`");
            stringBuilder.AppendLine("\t\t,`sync_row_is_tombstone`");
            stringBuilder.AppendLine("\t\t,`last_change_datetime`");

            var filterColumnsString = new StringBuilder();
            var filterColumnsString2 = new StringBuilder();
            var filterColumnsString3 = new StringBuilder();

            stringBuilder.AppendLine("\t) ");
            stringBuilder.AppendLine("\tVALUES (");
            stringBuilder.Append(stringBuilderArguments2.ToString());
            stringBuilder.AppendLine("\t\t,NULL");
            stringBuilder.AppendLine($"\t\t,{MySqlObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t\t,0");
            stringBuilder.AppendLine("\t\t,utc_timestamp()");


            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine("ON DUPLICATE KEY UPDATE");
            stringBuilder.AppendLine("\t`update_scope_id` = NULL, ");
            stringBuilder.AppendLine("\t`sync_row_is_tombstone` = 0, ");
            stringBuilder.AppendLine($"\t`timestamp` = {MySqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine("\t`last_change_datetime` = utc_timestamp();");

            stringBuilder.AppendLine("END IF;");

            stringBuilder.AppendLine($"End; ");
            return stringBuilder.ToString();
        }

        public DbCommand CreateUpdateTriggerCommand(DbConnection connection, DbTransaction transaction)
        {
            var updTriggerName = string.Format(this.mySqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger).name, tableName.Unquoted().Normalized().ToString());
            StringBuilder createTrigger = new StringBuilder();
            createTrigger.AppendLine($"DROP TRIGGER IF EXISTS {updTriggerName};");
            createTrigger.AppendLine($"CREATE TRIGGER {updTriggerName} AFTER UPDATE ON {tableName.Quoted().ToString()} FOR EACH ROW ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.UpdateTriggerBodyText());

            return new MySqlCommand(createTrigger.ToString(), (MySqlConnection)connection, (MySqlTransaction)transaction);
        }

        public Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {
            var commandTriggerType = triggerType switch
            {
                DbTriggerType.Delete => DbCommandType.DeleteTrigger,
                DbTriggerType.Insert => DbCommandType.InsertTrigger,
                DbTriggerType.Update => DbCommandType.UpdateTrigger,
                _ => throw new NotImplementedException()
            };
            var triggerNameString = string.Format(this.mySqlObjectNames.GetCommandName(commandTriggerType).name, tableName.Unquoted().Normalized().ToString());
            var triggerName = ParserName.Parse(triggerNameString, "`");

            var dbCommand = new MySqlCommand
            {
                CommandText = "select count(*) from information_schema.TRIGGERS where trigger_name = @triggerName AND trigger_schema = schema()"
            };

            dbCommand.Parameters.AddWithValue("@triggerName", triggerName.Unquoted().ToString());

            return Task.FromResult(dbCommand as DbCommand);

        }
        public Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {
            return triggerType switch
            {
                DbTriggerType.Delete => Task.FromResult(CreateDeleteTriggerCommand(connection, transaction)),
                DbTriggerType.Insert => Task.FromResult(CreateInsertTriggerCommand(connection, transaction)),
                DbTriggerType.Update => Task.FromResult(CreateUpdateTriggerCommand(connection, transaction)),
                _ => throw new NotImplementedException()
            };
        }
        public Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {
            var commandTriggerType = triggerType switch
            {
                DbTriggerType.Delete => DbCommandType.DeleteTrigger,
                DbTriggerType.Insert => DbCommandType.InsertTrigger,
                DbTriggerType.Update => DbCommandType.UpdateTrigger,
                _ => throw new NotImplementedException()
            };
            var triggerNameString = string.Format(this.mySqlObjectNames.GetCommandName(commandTriggerType).name, tableName.Unquoted().Normalized().ToString());
            var triggerName = ParserName.Parse(triggerNameString, "`");

            DbCommand dbCommand = connection.CreateCommand();
            
            dbCommand.CommandText = $"drop trigger {triggerName.Unquoted().ToString()}";

            return Task.FromResult(dbCommand);
        }
    }
}
