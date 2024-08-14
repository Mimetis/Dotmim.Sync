using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;
using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.PostgreSql.Builders
{
    /// <summary>
    /// Represents a trigger builder for PostgreSql.
    /// </summary>
    public class NpgsqlBuilderTrigger
    {
        /// <summary>
        /// Gets the table description.
        /// </summary>
        protected SyncTable TableDescription { get; }

        /// <summary>
        /// Gets the ,npgsql object names.
        /// </summary>
        protected NpgsqlObjectNames NpgsqlObjectNames { get; }

        /// <summary>
        /// Gets the npgsql database metadata.
        /// </summary>
        protected NpgsqlDbMetadata NpgsqlDbMetadata { get; }

        /// <inheritdoc cref="NpgsqlBuilderTrigger"/>
        public NpgsqlBuilderTrigger(SyncTable tableDescription, NpgsqlObjectNames npgsqlObjectNames, NpgsqlDbMetadata npgsqlDbMetadata)
        {
            this.TableDescription = tableDescription;
            this.NpgsqlObjectNames = npgsqlObjectNames;
            this.NpgsqlDbMetadata = npgsqlDbMetadata;
        }

        /// <summary>
        /// Returns a command to check if a trigger exists.
        /// </summary>
        public virtual Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {

            var commandTriggerName = this.NpgsqlObjectNames.GetTriggerName(triggerType);
            var commandTriggerParser = new ObjectParser(commandTriggerName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);

            var commandText = $"select exists(select * from information_schema.triggers where trigger_schema = @schemaname and trigger_name = @triggername )";

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = commandText;

            var p1 = command.CreateParameter();
            p1.ParameterName = "@triggername";
            p1.Value = commandTriggerParser.ObjectName.ToLowerInvariant();
            command.Parameters.Add(p1);

            var p2 = command.CreateParameter();
            p2.ParameterName = "@schemaname";
            p2.Value = this.NpgsqlObjectNames.TableSchemaName;
            command.Parameters.Add(p2);

            return Task.FromResult(command);
        }

        /// <summary>
        /// Returns a command to drop a trigger.
        /// </summary>
        public virtual Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {

            var commandTriggerName = this.NpgsqlObjectNames.GetTriggerName(triggerType);

            var function = $"\"{this.NpgsqlObjectNames.TableSchemaName}\".{commandTriggerName.ToLowerInvariant()}_function()";
            var commandText = $"drop trigger if exists {commandTriggerName.ToLowerInvariant()} on {this.NpgsqlObjectNames.TableQuotedFullName};" +
                              $"drop function if exists {function};";

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = commandText;

            return Task.FromResult(command);
        }

        /// <summary>
        /// Returns a command to create a trigger.
        /// </summary>
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

        /// <summary>
        /// Returns a command text to create a trigger.
        /// </summary>
        private string CreateInsertOrUpdateTriggerAsync(DbTriggerType triggerType)
        {

            var commandTriggerName = this.NpgsqlObjectNames.GetTriggerName(triggerType);

            string triggerFor = triggerType switch
            {
                DbTriggerType.Insert => "INSERT",
                DbTriggerType.Update => "UPDATE",
                _ => throw new NotImplementedException(),
            };

            var idColumnsSelects = new StringBuilder();
            var idColumns = new StringBuilder();
            string argComma = string.Empty;
            string argAnd = string.Empty;
            var primaryKeys = this.TableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                idColumns.Append($"{argComma}{columnParser.QuotedShortName}");
                idColumnsSelects.Append($"{argComma}NEW.{columnParser.QuotedShortName}");
                argComma = ",";
                argAnd = " AND ";
            }

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE OR REPLACE FUNCTION \"{this.NpgsqlObjectNames.TableSchemaName}\".{commandTriggerName.ToLowerInvariant()}_function()");
            stringBuilder.AppendLine($"  RETURNS trigger");
            stringBuilder.AppendLine($"  LANGUAGE 'plpgsql'");
            stringBuilder.AppendLine($"  COST 100");
            stringBuilder.AppendLine($"  VOLATILE NOT LEAKPROOF");
            stringBuilder.AppendLine($"AS $new$");
            stringBuilder.AppendLine($"BEGIN");
            stringBuilder.AppendLine($"  INSERT INTO {this.NpgsqlObjectNames.TrackingTableQuotedFullName} ");
            stringBuilder.AppendLine($"  ({idColumns}, \"update_scope_id\", \"timestamp\" ,\"sync_row_is_tombstone\" ,\"last_change_datetime\")");
            stringBuilder.AppendLine($"  VALUES( {idColumnsSelects}, null, {NpgsqlSyncAdapter.TimestampValue}, 0, now())");
            stringBuilder.AppendLine($"  ON CONFLICT({idColumns}) DO UPDATE");
            stringBuilder.AppendLine($"  SET \"timestamp\" = {NpgsqlSyncAdapter.TimestampValue}, \"sync_row_is_tombstone\" = 0, \"update_scope_id\" = null ,\"last_change_datetime\" = now();");
            stringBuilder.AppendLine($"return NEW;");
            stringBuilder.AppendLine($"END;");
            stringBuilder.AppendLine($"$new$;");
            stringBuilder.AppendLine($"CREATE OR REPLACE TRIGGER {commandTriggerName.ToLowerInvariant()}");
            stringBuilder.AppendLine($"AFTER {triggerFor} ON {this.NpgsqlObjectNames.TableQuotedFullName}");
            stringBuilder.AppendLine($"FOR EACH ROW EXECUTE FUNCTION \"{this.NpgsqlObjectNames.TableSchemaName}\".{commandTriggerName.ToLowerInvariant()}_function()");

            return stringBuilder.ToString();
        }

        /// <summary>
        /// Retruns a command text to create a delete trigger.
        /// </summary>
        private string CreateDeleteTriggerAsync()
        {
            var commandTriggerName = this.NpgsqlObjectNames.GetTriggerName(DbTriggerType.Delete);

            var idColumnsSelects = new StringBuilder();
            var idColumns = new StringBuilder();
            string argComma = string.Empty;
            string argAnd = string.Empty;
            var primaryKeys = this.TableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);

                idColumns.Append($"{argComma}{columnParser.QuotedShortName}");
                idColumnsSelects.Append($"{argComma}OLD.{columnParser.QuotedShortName}");
                argComma = ",";
                argAnd = " AND ";
            }

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE OR REPLACE FUNCTION \"{this.NpgsqlObjectNames.TableSchemaName}\".{commandTriggerName.ToLowerInvariant()}_function()");
            stringBuilder.AppendLine($"  RETURNS trigger");
            stringBuilder.AppendLine($"  LANGUAGE 'plpgsql'");
            stringBuilder.AppendLine($"  COST 100");
            stringBuilder.AppendLine($"  VOLATILE NOT LEAKPROOF");
            stringBuilder.AppendLine($"AS $new$");
            stringBuilder.AppendLine($"BEGIN");
            stringBuilder.AppendLine($"  INSERT INTO {this.NpgsqlObjectNames.TrackingTableQuotedFullName} ");
            stringBuilder.AppendLine($"  ({idColumns}, \"update_scope_id\", \"timestamp\" ,\"sync_row_is_tombstone\" ,\"last_change_datetime\")");
            stringBuilder.AppendLine($"  VALUES( {idColumnsSelects}, null, {NpgsqlSyncAdapter.TimestampValue}, 1, now())");
            stringBuilder.AppendLine($"  ON CONFLICT({idColumns}) DO UPDATE");
            stringBuilder.AppendLine($"  SET \"timestamp\" = {NpgsqlSyncAdapter.TimestampValue}, \"sync_row_is_tombstone\" = 1, \"update_scope_id\" = null ,\"last_change_datetime\" = now();");
            stringBuilder.AppendLine($"return OLD;");
            stringBuilder.AppendLine($"END;");
            stringBuilder.AppendLine($"$new$;");
            stringBuilder.AppendLine($"CREATE OR REPLACE TRIGGER {commandTriggerName.ToLowerInvariant()}");
            stringBuilder.AppendLine($"AFTER DELETE ON {this.NpgsqlObjectNames.TableQuotedFullName}");
            stringBuilder.AppendLine($"FOR EACH ROW EXECUTE FUNCTION \"{this.NpgsqlObjectNames.TableSchemaName}\".{commandTriggerName.ToLowerInvariant()}_function()");
            return stringBuilder.ToString();
        }
    }
}