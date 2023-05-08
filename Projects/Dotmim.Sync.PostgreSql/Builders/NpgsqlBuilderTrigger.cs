using Dotmim.Sync.Builders;
using Dotmim.Sync.PostgreSql.Scope;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.PostgreSql.Builders
{
    public class NpgsqlBuilderTrigger
    {
        private string scopeName;
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
            this.scopeName = scopeName;
            this.timestampValue = NpgsqlSyncAdapter.TimestampValue;
        }


        private ParserName GetTriggerName(DbTriggerType triggerType)
        {
            var tpref = this.setup?.TriggersPrefix;
            var tsuf = this.setup?.TriggersSuffix;

            var triggerName = $"{tpref}{tableName.Unquoted().Normalized()}{tsuf}_";

            var commandTriggerName = triggerType switch
            {
                DbTriggerType.Insert => string.Format(NpgsqlSyncAdapter.insertTriggerName, triggerName),
                DbTriggerType.Update => string.Format(NpgsqlSyncAdapter.updateTriggerName, triggerName),
                DbTriggerType.Delete => string.Format(NpgsqlSyncAdapter.deleteTriggerName, triggerName),
                _ => null
            };
            return ParserName.Parse(commandTriggerName, "\"");

        }
        public virtual Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {

            var commandTriggerName = GetTriggerName(triggerType);

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
            p2.Value = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);
            command.Parameters.Add(p2);

            return Task.FromResult(command);

        }
        public virtual Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {
            var commandTriggerName = GetTriggerName(triggerType);
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);
            var function = $"\"{schema}\".{commandTriggerName.Unquoted().Normalized().ToString().ToLower()}_function()";
            var commandText = $"drop trigger if exists {commandTriggerName.Quoted()} on \"{schema}\".{tableName.Quoted()};" +
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
                DbTriggerType.Insert => CreateInsertOrUpdateTriggerAsync(triggerType),
                DbTriggerType.Update => CreateInsertOrUpdateTriggerAsync(triggerType),
                DbTriggerType.Delete => CreateDeleteTriggerAsync(),
                _ => throw new NotImplementedException()
            };

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = commandTriggerFunctionString;
            return Task.FromResult(command);
        }

        private string CreateInsertOrUpdateTriggerAsync(DbTriggerType triggerType)
        {
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);
            var commandTriggerName = GetTriggerName(triggerType);

            string triggerFor = triggerType switch
            {
                DbTriggerType.Insert => "INSERT",
                DbTriggerType.Update => "UPDATE",
                DbTriggerType.Delete => "DELETE",
                _ => throw new NotImplementedException()
            };

            var idColumnsSelects = new StringBuilder();
            var idColumns = new StringBuilder();
            string argComma = "";
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                idColumns.Append(argComma).Append(columnName);
                idColumnsSelects.Append(argComma).Append("NEW.").Append(columnName);
                argComma = ",";
                argAnd = " AND ";
            }

            var stringBuilder = new StringBuilder();
            stringBuilder.Append("CREATE OR REPLACE FUNCTION \"").Append(schema).Append("\".").Append(commandTriggerName.Unquoted().Normalized().ToString().ToLower()).AppendLine("_function()");
            stringBuilder.AppendLine($"  RETURNS trigger");
            stringBuilder.AppendLine($"  LANGUAGE 'plpgsql'");
            stringBuilder.AppendLine($"  COST 100");
            stringBuilder.AppendLine($"  VOLATILE NOT LEAKPROOF");
            stringBuilder.AppendLine($"AS $new$");
            stringBuilder.AppendLine($"BEGIN");
            stringBuilder.Append("  INSERT INTO \"").Append(schema).Append("\".").Append(trackingName.Quoted()).AppendLine(" ");
            stringBuilder.Append("  (").Append(idColumns).AppendLine(", \"update_scope_id\", \"timestamp\" ,\"sync_row_is_tombstone\" ,\"last_change_datetime\")");
            stringBuilder.Append("  VALUES( ").Append(idColumnsSelects).Append(", null, ").Append(this.timestampValue).AppendLine(", 0, now())");
            stringBuilder.Append("  ON CONFLICT(").Append(idColumns).AppendLine(") DO UPDATE");
            stringBuilder.Append("  SET \"timestamp\" = ").Append(this.timestampValue).AppendLine(", \"sync_row_is_tombstone\" = 0, \"update_scope_id\" = null ,\"last_change_datetime\" = now();");
            stringBuilder.AppendLine($"return NEW;");
            stringBuilder.AppendLine($"END;");
            stringBuilder.AppendLine($"$new$;");
            stringBuilder.Append("CREATE OR REPLACE TRIGGER ").Append(commandTriggerName.Quoted()).AppendLine();
            stringBuilder.Append("AFTER ").Append(triggerFor).Append(" ON \"").Append(schema).Append("\".").Append(tableName.Quoted()).AppendLine();
            stringBuilder.Append("FOR EACH ROW EXECUTE FUNCTION \"").Append(schema).Append("\".").Append(commandTriggerName.Unquoted().Normalized().ToString().ToLower()).AppendLine("_function()");

            return stringBuilder.ToString(); ;
        }
    
        private string CreateDeleteTriggerAsync()
        {
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);
            var commandTriggerName = GetTriggerName(DbTriggerType.Delete);

            var idColumnsSelects = new StringBuilder();
            var idColumns = new StringBuilder();
            string argComma = "";
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                idColumns.Append(argComma).Append(columnName);
                idColumnsSelects.Append(argComma).Append("OLD.").Append(columnName);
                argComma = ",";
                argAnd = " AND ";
            }

            var stringBuilder = new StringBuilder();
            stringBuilder.Append("CREATE OR REPLACE FUNCTION \"").Append(schema).Append("\".").Append(commandTriggerName.Unquoted().Normalized().ToString().ToLower()).AppendLine("_function()");
            stringBuilder.AppendLine($"  RETURNS trigger");
            stringBuilder.AppendLine($"  LANGUAGE 'plpgsql'");
            stringBuilder.AppendLine($"  COST 100");
            stringBuilder.AppendLine($"  VOLATILE NOT LEAKPROOF");
            stringBuilder.AppendLine($"AS $new$");
            stringBuilder.AppendLine($"BEGIN");
            stringBuilder.Append("  INSERT INTO \"").Append(schema).Append("\".").Append(trackingName.Quoted()).AppendLine(" ");
            stringBuilder.Append("  (").Append(idColumns).AppendLine(", \"update_scope_id\", \"timestamp\" ,\"sync_row_is_tombstone\" ,\"last_change_datetime\")");
            stringBuilder.Append("  VALUES( ").Append(idColumnsSelects).Append(", null, ").Append(this.timestampValue).AppendLine(", 1, now())");
            stringBuilder.Append("  ON CONFLICT(").Append(idColumns).AppendLine(") DO UPDATE");
            stringBuilder.Append("  SET \"timestamp\" = ").Append(this.timestampValue).AppendLine(", \"sync_row_is_tombstone\" = 1, \"update_scope_id\" = null ,\"last_change_datetime\" = now();");
            stringBuilder.AppendLine($"return OLD;");
            stringBuilder.AppendLine($"END;");
            stringBuilder.AppendLine($"$new$;");
            stringBuilder.Append("CREATE OR REPLACE TRIGGER ").Append(commandTriggerName.Quoted()).AppendLine();
            stringBuilder.Append("AFTER DELETE ON \"").Append(schema).Append("\".").Append(tableName.Quoted()).AppendLine();
            stringBuilder.Append("FOR EACH ROW EXECUTE FUNCTION \"").Append(schema).Append("\".").Append(commandTriggerName.Unquoted().Normalized().ToString().ToLower()).AppendLine("_function()");
            return stringBuilder.ToString();

        }
    }
}