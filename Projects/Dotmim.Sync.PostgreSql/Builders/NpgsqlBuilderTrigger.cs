using Dotmim.Sync.Builders;
using Dotmim.Sync.Postgres;
using Dotmim.Sync.Postgres.Scope;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Postgres.Builders
{
    public class SqlBuilderTrigger : IDbBuilderTriggerHelper
    {
        private ParserName tableName;
        private ParserName trackingName;
        private readonly SyncTable tableDescription;
        private readonly SyncSetup setup;
        private readonly NpgsqlObjectNames sqlObjectNames;

        public SqlBuilderTrigger(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
        {
            this.tableDescription = tableDescription;
            this.setup = setup;
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.sqlObjectNames = new NpgsqlObjectNames(this.tableDescription, this.setup);
        }

        private string DeleteTriggerBodyText()
        {
            var stringBuilder = new StringBuilder();


            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringBuilderArguments3 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();

            string argComma = " ";
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                stringBuilderArguments.AppendLine($"\t{argComma}OLD.{columnName}");
                stringBuilderArguments2.AppendLine($"\t{argComma}{columnName}");
                stringBuilderArguments3.Append($"{argComma}{columnName}");
                stringPkAreNull.Append($"{argAnd}side.{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.AppendLine($"INSERT INTO {trackingName.Schema().Quoted().ToString()} (");
            stringBuilder.Append(stringBuilderArguments2.ToString());
            stringBuilder.AppendLine($"\t,update_scope_id");
            stringBuilder.AppendLine($"\t,sync_row_is_tombstone");
            stringBuilder.AppendLine($"\t,last_change_datetime");
            stringBuilder.AppendLine($"\t,timestamp");
            stringBuilder.AppendLine($") ");
            stringBuilder.AppendLine($"VALUES (");
            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine($"\t,NULL");
            stringBuilder.AppendLine($"\t,true");
            stringBuilder.AppendLine($"\t,now()");
            stringBuilder.AppendLine($"\t,{NpgsqlScopeInfoBuilder.TimestampValue}");
            stringBuilder.AppendLine($")");
            stringBuilder.AppendLine($"ON CONFLICT({stringBuilderArguments3.ToString()})");
            stringBuilder.AppendLine($"DO UPDATE SET");
            stringBuilder.AppendLine($"\tsync_row_is_tombstone = true");
            stringBuilder.AppendLine($"\t,update_scope_id = NULL");
            stringBuilder.AppendLine($"\t,last_change_datetime = now()");
            stringBuilder.AppendLine($"\t,timestamp = {NpgsqlScopeInfoBuilder.TimestampValue};");

            return stringBuilder.ToString();
        }

        public virtual async Task CreateDeleteTriggerAsync(DbConnection connection, DbTransaction transaction)
        {
            var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger).name;
            var delTriggerParseName = ParserName.Parse(delTriggerName, "\"");
            var trigger = new StringBuilder();
            trigger.AppendLine($"CREATE OR REPLACE FUNCTION {delTriggerName}()");
            trigger.AppendLine($"RETURNS TRIGGER AS");
            trigger.AppendLine($"$BODY$");
            trigger.AppendLine($"BEGIN");
            trigger.AppendLine(this.DeleteTriggerBodyText());
            trigger.AppendLine($"RETURN NULL;");
            trigger.AppendLine($"END;");
            trigger.AppendLine($"$BODY$");
            trigger.AppendLine($"lANGUAGE 'plpgsql';");
            trigger.AppendLine($"DROP TRIGGER IF EXISTS {delTriggerParseName.Quoted().ToString()} on {tableName.Schema().Quoted().ToString()};");
            trigger.AppendLine($"CREATE TRIGGER {delTriggerParseName.Quoted().ToString()} AFTER DELETE ON {tableName.Schema().Quoted().ToString()}");
            trigger.AppendLine($"FOR EACH ROW EXECUTE FUNCTION {delTriggerName}();");
            trigger.AppendLine($"");

            using (var command = new NpgsqlCommand(trigger.ToString(), (NpgsqlConnection)connection, (NpgsqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }

        }
        public virtual async Task DropDeleteTriggerAsync(DbConnection connection, DbTransaction transaction)
        {
            var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger).name;
            var commandText = $"DROP TRIGGER {delTriggerName};";
            using (var command = new NpgsqlCommand(commandText, (NpgsqlConnection)connection, (NpgsqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        public virtual async Task AlterDeleteTriggerAsync(DbConnection connection, DbTransaction transaction)
        {
            var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger).name;
            var createTrigger = new StringBuilder($"ALTER TRIGGER {delTriggerName} ON {tableName.Schema().Quoted().ToString()} FOR DELETE AS ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.DeleteTriggerBodyText());

            using (var command = new NpgsqlCommand(createTrigger.ToString(), (NpgsqlConnection)connection, (NpgsqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }

        }

        private string InsertTriggerBodyText()
        {
            var stringBuilder = new StringBuilder();


            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringBuilderArguments3 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();

            string argComma = " ";
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                stringBuilderArguments.AppendLine($"\t{argComma}NEW.{columnName}");
                stringBuilderArguments2.AppendLine($"\t{argComma}{columnName}");
                stringBuilderArguments3.Append($"{argComma}{columnName}");
                stringPkAreNull.Append($"{argAnd}side.{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.AppendLine($"INSERT INTO {trackingName.Schema().Quoted().ToString()} (");
            stringBuilder.Append(stringBuilderArguments2.ToString());
            stringBuilder.AppendLine($"\t,update_scope_id");
            stringBuilder.AppendLine($"\t,sync_row_is_tombstone");
            stringBuilder.AppendLine($"\t,last_change_datetime");
            stringBuilder.AppendLine($"\t,timestamp");
            stringBuilder.AppendLine($") ");
            stringBuilder.AppendLine($"VALUES (");
            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine($"\t,NULL");
            stringBuilder.AppendLine($"\t,false");
            stringBuilder.AppendLine($"\t,now()");
            stringBuilder.AppendLine($"\t,{NpgsqlScopeInfoBuilder.TimestampValue}");
            stringBuilder.AppendLine($")");
            stringBuilder.AppendLine($"ON CONFLICT({stringBuilderArguments3.ToString()})");
            stringBuilder.AppendLine($"DO UPDATE SET");
            stringBuilder.AppendLine($"\tsync_row_is_tombstone = false");
            stringBuilder.AppendLine($"\t,update_scope_id = NULL");
            stringBuilder.AppendLine($"\t,last_change_datetime = now()");
            stringBuilder.AppendLine($"\t,timestamp = {NpgsqlScopeInfoBuilder.TimestampValue};");

            return stringBuilder.ToString();
        }
        public virtual async Task CreateInsertTriggerAsync(DbConnection connection, DbTransaction transaction)
        {
            var insTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger).name;
            var insTriggerParseName = ParserName.Parse(insTriggerName, "\"");
            var trigger = new StringBuilder();
            trigger.AppendLine($"CREATE OR REPLACE FUNCTION {insTriggerName}()");
            trigger.AppendLine($"RETURNS TRIGGER AS");
            trigger.AppendLine($"$BODY$");
            trigger.AppendLine($"BEGIN");
            trigger.AppendLine(this.InsertTriggerBodyText());
            trigger.AppendLine($"RETURN NULL;");
            trigger.AppendLine($"END;");
            trigger.AppendLine($"$BODY$");
            trigger.AppendLine($"lANGUAGE 'plpgsql';");
            trigger.AppendLine($"DROP TRIGGER IF EXISTS {insTriggerParseName.Quoted().ToString()} on {tableName.Schema().Quoted().ToString()};");
            trigger.AppendLine($"CREATE TRIGGER {insTriggerParseName.Quoted().ToString()} AFTER INSERT ON {tableName.Schema().Quoted().ToString()}");
            trigger.AppendLine($"FOR EACH ROW EXECUTE FUNCTION {insTriggerName}();");
            trigger.AppendLine($"");
            using (var command = new NpgsqlCommand(trigger.ToString(), (NpgsqlConnection)connection, (NpgsqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);

            }
        }

        public virtual async Task DropInsertTriggerAsync(DbConnection connection, DbTransaction transaction)
        {
            var triggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger).name;
            var commandText = $"DROP TRIGGER {triggerName};";

            using (var command = new NpgsqlCommand(commandText, (NpgsqlConnection)connection, (NpgsqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        public virtual async Task AlterInsertTriggerAsync(DbConnection connection, DbTransaction transaction)
        {
            var insTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger).name;
            var createTrigger = new StringBuilder($"ALTER TRIGGER {insTriggerName} ON {tableName.Schema().Quoted().ToString()} FOR INSERT AS ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());
            var commandText = createTrigger.ToString();

            using (var command = new NpgsqlCommand(commandText, (NpgsqlConnection)connection, (NpgsqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        private string UpdateTriggerBodyText()
        {
            var stringBuilder = new StringBuilder();


            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringBuilderArguments3 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();

            string argComma = " ";
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                stringBuilderArguments.AppendLine($"\t{argComma}NEW.{columnName}");
                stringBuilderArguments2.AppendLine($"\t{argComma}{columnName}");
                stringBuilderArguments3.Append($"{argComma}{columnName}");
                stringPkAreNull.Append($"{argAnd}side.{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.AppendLine($"INSERT INTO {trackingName.Schema().Quoted().ToString()} (");
            stringBuilder.Append(stringBuilderArguments2.ToString());
            stringBuilder.AppendLine($"\t,update_scope_id");
            stringBuilder.AppendLine($"\t,last_change_datetime");
            stringBuilder.AppendLine($"\t,timestamp");
            stringBuilder.AppendLine($") ");
            stringBuilder.AppendLine($"VALUES (");
            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine($"\t,NULL");
            stringBuilder.AppendLine($"\t,now()");
            stringBuilder.AppendLine($"\t,{NpgsqlScopeInfoBuilder.TimestampValue}");
            stringBuilder.AppendLine($")");
            stringBuilder.AppendLine($"ON CONFLICT({stringBuilderArguments3.ToString()})");
            stringBuilder.AppendLine($"DO UPDATE SET");
            stringBuilder.AppendLine($"\tsync_row_is_tombstone = false");
            stringBuilder.AppendLine($"\t,update_scope_id = NULL");
            stringBuilder.AppendLine($"\t,last_change_datetime = now()");
            stringBuilder.AppendLine($"\t,timestamp = {NpgsqlScopeInfoBuilder.TimestampValue};");

            return stringBuilder.ToString();
        }
        public virtual async Task CreateUpdateTriggerAsync(DbConnection connection, DbTransaction transaction)
        {
            var updTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger).name;
            var updTriggerParseName = ParserName.Parse(updTriggerName, "\"");
            var trigger = new StringBuilder();
            trigger.AppendLine($"CREATE OR REPLACE FUNCTION {updTriggerName}()");
            trigger.AppendLine($"RETURNS TRIGGER AS");
            trigger.AppendLine($"$BODY$");
            trigger.AppendLine($"BEGIN");
            trigger.AppendLine(this.UpdateTriggerBodyText());
            trigger.AppendLine($"RETURN NULL;");
            trigger.AppendLine($"END;");
            trigger.AppendLine($"$BODY$");
            trigger.AppendLine($"lANGUAGE 'plpgsql';");
            trigger.AppendLine($"DROP TRIGGER IF EXISTS {updTriggerParseName.Quoted().ToString()} on {tableName.Schema().Quoted().ToString()};");
            trigger.AppendLine($"CREATE TRIGGER {updTriggerParseName.Quoted().ToString()} AFTER UPDATE ON {tableName.Schema().Quoted().ToString()}");
            trigger.AppendLine($"FOR EACH ROW EXECUTE FUNCTION {updTriggerName}();");
            trigger.AppendLine($"");

            using (var command = new NpgsqlCommand(trigger.ToString(), (NpgsqlConnection)connection, (NpgsqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        public virtual async Task DropUpdateTriggerAsync(DbConnection connection, DbTransaction transaction)
        {
            var triggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger).name;
            var commandText = $"DROP TRIGGER {triggerName};";
            using (var command = new NpgsqlCommand(commandText, (NpgsqlConnection)connection, (NpgsqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }


        public virtual async Task AlterUpdateTriggerAsync(DbConnection connection, DbTransaction transaction)
        {
            var updTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger).name;
            var createTrigger = new StringBuilder($"ALTER TRIGGER {updTriggerName} ON {tableName.Schema().Quoted().ToString()} FOR UPDATE AS ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.UpdateTriggerBodyText());
            var commandText = createTrigger.ToString();

            using (var command = new NpgsqlCommand(commandText, (NpgsqlConnection)connection, (NpgsqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        public virtual async Task<bool> NeedToCreateTriggerAsync(DbTriggerType type, DbConnection connection, DbTransaction transaction)
        {

            var updTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger).name;
            var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger).name;
            var insTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger).name;

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

            return !await NpgsqlManagementUtils.TriggerExistsAsync((NpgsqlConnection)connection, (NpgsqlTransaction)transaction, triggerName).ConfigureAwait(false);
        }


    }
}