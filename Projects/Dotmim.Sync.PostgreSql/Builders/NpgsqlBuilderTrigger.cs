using Dotmim.Sync.Builders;
using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.PostgreSql.Builders
{
    public class NpgsqlBuilderTrigger
    {
        private SyncSetup setup;
        private SyncTable tableDescription;
        private ParserName tableName;
        private ParserName trackingName;
        private string timestampValue;

        public NpgsqlBuilderTrigger(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
        {
            this.tableDescription = tableDescription;
            this.tableName = tableName;
            this.trackingName = trackingTableName;
            this.setup = setup;
            this.timestampValue = NpgsqlSyncAdapter.TimestampValue;
        }

        private ParserName GetTriggerName(DbTriggerType triggerType)
        {
            var tpref = this.setup?.TriggersPrefix;
            var tsuf = this.setup?.TriggersSuffix;

            var triggerName = $"{tpref}{this.tableName.Unquoted().Normalized()}{tsuf}_";

            var commandTriggerName = triggerType switch
            {
                DbTriggerType.Insert => string.Format(NpgsqlSyncAdapter.InsertTriggerName, triggerName),
                DbTriggerType.Update => string.Format(NpgsqlSyncAdapter.UpdateTriggerName, triggerName),
                DbTriggerType.Delete => string.Format(NpgsqlSyncAdapter.DeleteTriggerName, triggerName),
                _ => null,
            };
            return ParserName.Parse(commandTriggerName, "\"");
        }

        public virtual Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {

            var commandTriggerName = this.GetTriggerName(triggerType);

            var commandText = $"select exists(select * from information_schema.triggers where trigger_schema = @schemaname and trigger_name = @triggername )";

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = commandText;

            var p1 = command.CreateParameter();
            p1.ParameterName = "@triggername";
            p1.Value = commandTriggerName.Unquoted().Normalized().ToString();
            command.Parameters.Add(p1);

            var p2 = command.CreateParameter();
            p2.ParameterName = "@schemaname";
            p2.Value = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(this.tableName);
            command.Parameters.Add(p2);

            return Task.FromResult(command);
        }

        public virtual Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {
            var commandTriggerName = this.GetTriggerName(triggerType);
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(this.tableName);
            var function = $"\"{schema}\".{commandTriggerName.Unquoted().Normalized().ToString().ToLower(System.Globalization.CultureInfo.CurrentCulture)}_function()";
            var commandText = $"drop trigger if exists {commandTriggerName.Quoted()} on \"{schema}\".{this.tableName.Quoted()};" +
                              $"drop function if exists {function};";

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = commandText;

            return Task.FromResult(command);
        }

        public virtual Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {
            var commandTriggerFunctionString = triggerType switch
            {
                DbTriggerType.Insert => this.CreateInsertOrUpdateTriggerAsync(triggerType),
                DbTriggerType.Update => this.CreateInsertOrUpdateTriggerAsync(triggerType),
                DbTriggerType.Delete => this.CreateDeleteTriggerAsync(),
                _ => throw new NotImplementedException(),
            };

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = commandTriggerFunctionString;
            return Task.FromResult(command);
        }

        private string CreateInsertOrUpdateTriggerAsync(DbTriggerType triggerType)
        {
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(this.tableName);
            var commandTriggerName = this.GetTriggerName(triggerType);

            string triggerFor = triggerType switch
            {
                DbTriggerType.Insert => "INSERT",
                DbTriggerType.Update => "UPDATE",
                DbTriggerType.Delete => "DELETE",
                _ => throw new NotImplementedException(),
            };

            var idColumnsSelects = new StringBuilder();
            var idColumns = new StringBuilder();
            string argComma = string.Empty;
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                idColumns.Append($"{argComma}{columnName}");
                idColumnsSelects.Append($"{argComma}NEW.{columnName}");
                argComma = ",";
                argAnd = " AND ";
            }

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE OR REPLACE FUNCTION \"{schema}\".{commandTriggerName.Unquoted().Normalized().ToString().ToLower(System.Globalization.CultureInfo.CurrentCulture)}_function()");
            stringBuilder.AppendLine($"  RETURNS trigger");
            stringBuilder.AppendLine($"  LANGUAGE 'plpgsql'");
            stringBuilder.AppendLine($"  COST 100");
            stringBuilder.AppendLine($"  VOLATILE NOT LEAKPROOF");
            stringBuilder.AppendLine($"AS $new$");
            stringBuilder.AppendLine($"BEGIN");
            stringBuilder.AppendLine($"  INSERT INTO \"{schema}\".{this.trackingName.Quoted()} ");
            stringBuilder.AppendLine($"  ({idColumns}, \"update_scope_id\", \"timestamp\" ,\"sync_row_is_tombstone\" ,\"last_change_datetime\")");
            stringBuilder.AppendLine($"  VALUES( {idColumnsSelects}, null, {this.timestampValue}, 0, now())");
            stringBuilder.AppendLine($"  ON CONFLICT({idColumns}) DO UPDATE");
            stringBuilder.AppendLine($"  SET \"timestamp\" = {this.timestampValue}, \"sync_row_is_tombstone\" = 0, \"update_scope_id\" = null ,\"last_change_datetime\" = now();");
            stringBuilder.AppendLine($"return NEW;");
            stringBuilder.AppendLine($"END;");
            stringBuilder.AppendLine($"$new$;");
            stringBuilder.AppendLine($"CREATE OR REPLACE TRIGGER {commandTriggerName.Quoted()}");
            stringBuilder.AppendLine($"AFTER {triggerFor} ON \"{schema}\".{this.tableName.Quoted()}");
            stringBuilder.AppendLine($"FOR EACH ROW EXECUTE FUNCTION \"{schema}\".{commandTriggerName.Unquoted().Normalized().ToString().ToLower(System.Globalization.CultureInfo.CurrentCulture)}_function()");

            return stringBuilder.ToString();
        }

        private string CreateDeleteTriggerAsync()
        {
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(this.tableName);
            var commandTriggerName = this.GetTriggerName(DbTriggerType.Delete);

            var idColumnsSelects = new StringBuilder();
            var idColumns = new StringBuilder();
            string argComma = string.Empty;
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                idColumns.Append($"{argComma}{columnName}");
                idColumnsSelects.Append($"{argComma}OLD.{columnName}");
                argComma = ",";
                argAnd = " AND ";
            }

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE OR REPLACE FUNCTION \"{schema}\".{commandTriggerName.Unquoted().Normalized().ToString().ToLower(System.Globalization.CultureInfo.CurrentCulture)}_function()");
            stringBuilder.AppendLine($"  RETURNS trigger");
            stringBuilder.AppendLine($"  LANGUAGE 'plpgsql'");
            stringBuilder.AppendLine($"  COST 100");
            stringBuilder.AppendLine($"  VOLATILE NOT LEAKPROOF");
            stringBuilder.AppendLine($"AS $new$");
            stringBuilder.AppendLine($"BEGIN");
            stringBuilder.AppendLine($"  INSERT INTO \"{schema}\".{this.trackingName.Quoted()} ");
            stringBuilder.AppendLine($"  ({idColumns}, \"update_scope_id\", \"timestamp\" ,\"sync_row_is_tombstone\" ,\"last_change_datetime\")");
            stringBuilder.AppendLine($"  VALUES( {idColumnsSelects}, null, {this.timestampValue}, 1, now())");
            stringBuilder.AppendLine($"  ON CONFLICT({idColumns}) DO UPDATE");
            stringBuilder.AppendLine($"  SET \"timestamp\" = {this.timestampValue}, \"sync_row_is_tombstone\" = 1, \"update_scope_id\" = null ,\"last_change_datetime\" = now();");
            stringBuilder.AppendLine($"return OLD;");
            stringBuilder.AppendLine($"END;");
            stringBuilder.AppendLine($"$new$;");
            stringBuilder.AppendLine($"CREATE OR REPLACE TRIGGER {commandTriggerName.Quoted()}");
            stringBuilder.AppendLine($"AFTER DELETE ON \"{schema}\".{this.tableName.Quoted()}");
            stringBuilder.AppendLine($"FOR EACH ROW EXECUTE FUNCTION \"{schema}\".{commandTriggerName.Unquoted().Normalized().ToString().ToLower(System.Globalization.CultureInfo.CurrentCulture)}_function()");
            return stringBuilder.ToString();
        }
    }
}