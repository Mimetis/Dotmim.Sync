using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;

#if NET6_0 || NET8_0_OR_GREATER
using MySqlConnector;
#elif NETSTANDARD
using MySql.Data.MySqlClient;
#endif
using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

#if MARIADB
namespace Dotmim.Sync.MariaDB.Builders
#elif MYSQL
namespace Dotmim.Sync.MySql.Builders
#endif
{
    /// <summary>
    /// Represents a MySql builder for triggers.
    /// </summary>
    public class MySqlBuilderTrigger
    {
        /// <summary>
        /// Gets the table description.
        /// </summary>
        protected SyncTable TableDescription { get; }

        /// <summary>
        /// Gets the MySql object names.
        /// </summary>
        protected MySqlObjectNames MySqlObjectNames { get; }

        /// <summary>
        /// Gets the MySql database metadata.
        /// </summary>
        protected MySqlDbMetadata MySqlDbMetadata { get; }

        /// <inheritdoc cref="MySqlBuilderTrigger"/>
        public MySqlBuilderTrigger(SyncTable tableDescription, MySqlObjectNames mysqlObjectNames, MySqlDbMetadata mysqlDbMetadata)
        {
            this.TableDescription = tableDescription;
            this.MySqlObjectNames = mysqlObjectNames;
            this.MySqlDbMetadata = mysqlDbMetadata;
        }

        /// <summary>
        /// Returns a command to delete a trigger.
        /// </summary>
        public DbCommand CreateDeleteTriggerCommand(DbConnection connection, DbTransaction transaction)
        {
            var triggerName = this.MySqlObjectNames.GetTriggerCommandName(DbTriggerType.Delete);

            StringBuilder createTrigger = new StringBuilder();
            createTrigger.AppendLine($"CREATE TRIGGER {triggerName} AFTER DELETE ON {this.MySqlObjectNames.TableQuotedShortName} FOR EACH ROW ");
            createTrigger.AppendLine();
            createTrigger.AppendLine();
            createTrigger.AppendLine("BEGIN");

            createTrigger.AppendLine($"\tINSERT INTO {this.MySqlObjectNames.TrackingTableQuotedShortName} (");

            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderArguments2 = new StringBuilder();
            StringBuilder stringPkAreNull = new StringBuilder();

            string argComma = string.Empty;
            string argAnd = string.Empty;
            foreach (var mutableColumn in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);

                stringBuilderArguments.AppendLine($"\t\t{argComma}{columnParser.QuotedShortName}");
                stringBuilderArguments2.AppendLine($"\t\t{argComma}old.{columnParser.QuotedShortName}");
                stringPkAreNull.Append($"{argAnd}{this.MySqlObjectNames.TrackingTableQuotedShortName}.{columnParser.QuotedShortName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            createTrigger.Append(stringBuilderArguments);
            createTrigger.AppendLine("\t\t,`update_scope_id`");
            createTrigger.AppendLine("\t\t,`timestamp`");
            createTrigger.AppendLine("\t\t,`sync_row_is_tombstone`");
            createTrigger.AppendLine("\t\t,`last_change_datetime`");

            var filterColumnsString = new StringBuilder();
            var filterColumnsString2 = new StringBuilder();
            var filterColumnsString3 = new StringBuilder();

            createTrigger.AppendLine("\t) ");
            createTrigger.AppendLine("\tVALUES (");
            createTrigger.Append(stringBuilderArguments2);
            createTrigger.AppendLine("\t\t,NULL");
            createTrigger.AppendLine($"\t\t,{MySqlObjectNames.TimestampValue}");
            createTrigger.AppendLine("\t\t,1");
            createTrigger.AppendLine("\t\t,utc_timestamp()");

            createTrigger.AppendLine("\t)");
            createTrigger.AppendLine("ON DUPLICATE KEY UPDATE");
            createTrigger.AppendLine("\t`update_scope_id` = NULL, ");
            createTrigger.AppendLine("\t`sync_row_is_tombstone` = 1, ");
            createTrigger.AppendLine($"\t`timestamp` = {MySqlObjectNames.TimestampValue}, ");
            createTrigger.AppendLine("\t`last_change_datetime` = utc_timestamp()");

            createTrigger.Append(";");
            createTrigger.AppendLine("END");

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = createTrigger.ToString();

            return command;
        }

        /// <summary>
        /// Returns a command to create an insert table trigger.
        /// </summary>
        public DbCommand CreateInsertTriggerCommand(DbConnection connection, DbTransaction transaction)
        {
            var insTriggerName = this.MySqlObjectNames.GetTriggerCommandName(DbTriggerType.Insert);

            StringBuilder createTrigger = new StringBuilder();
            createTrigger.AppendLine($"CREATE TRIGGER {insTriggerName} AFTER INSERT ON {this.MySqlObjectNames.TableQuotedShortName} FOR EACH ROW ");
            createTrigger.AppendLine();
            createTrigger.AppendLine();
            createTrigger.AppendLine("-- If row was deleted before, it already exists, so just make an update");
            createTrigger.AppendLine("BEGIN");

            createTrigger.AppendLine($"\tINSERT INTO {this.MySqlObjectNames.TrackingTableQuotedShortName} (");

            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();

            string argComma = string.Empty;
            string argAnd = string.Empty;
            foreach (var mutableColumn in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);

                stringBuilderArguments.AppendLine($"\t\t{argComma}{columnParser.QuotedShortName}");
                stringBuilderArguments2.AppendLine($"\t\t{argComma}new.{columnParser.QuotedShortName}");
                stringPkAreNull.Append($"{argAnd}{this.MySqlObjectNames.TrackingTableQuotedShortName}.{columnParser.QuotedShortName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            createTrigger.Append(stringBuilderArguments);
            createTrigger.AppendLine("\t\t,`update_scope_id`");
            createTrigger.AppendLine("\t\t,`timestamp`");
            createTrigger.AppendLine("\t\t,`sync_row_is_tombstone`");
            createTrigger.AppendLine("\t\t,`last_change_datetime`");

            var filterColumnsString = new StringBuilder();
            var filterColumnsString2 = new StringBuilder();
            var filterColumnsString3 = new StringBuilder();

            createTrigger.AppendLine("\t) ");
            createTrigger.AppendLine("\tVALUES (");
            createTrigger.Append(stringBuilderArguments2);
            createTrigger.AppendLine("\t\t,NULL");
            createTrigger.AppendLine($"\t\t,{MySqlObjectNames.TimestampValue}");
            createTrigger.AppendLine("\t\t,0");
            createTrigger.AppendLine("\t\t,utc_timestamp()");

            createTrigger.AppendLine("\t)");
            createTrigger.AppendLine("ON DUPLICATE KEY UPDATE");
            createTrigger.AppendLine("\t`update_scope_id` = NULL, ");
            createTrigger.AppendLine("\t`sync_row_is_tombstone` = 0, ");
            createTrigger.AppendLine($"\t`timestamp` = {MySqlObjectNames.TimestampValue}, ");
            createTrigger.AppendLine("\t`last_change_datetime` = utc_timestamp()");

            createTrigger.Append(";");
            createTrigger.AppendLine("END");

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = createTrigger.ToString();

            return command;
        }

        /// <summary>
        /// Returns a command to create an update table trigger.
        /// </summary>
        public DbCommand CreateUpdateTriggerCommand(DbConnection connection, DbTransaction transaction)
        {
            var updTriggerName = this.MySqlObjectNames.GetTriggerCommandName(DbTriggerType.Update);

            StringBuilder createTrigger = new StringBuilder();
            createTrigger.AppendLine($"CREATE TRIGGER {updTriggerName} AFTER UPDATE ON {this.MySqlObjectNames.TableQuotedShortName} FOR EACH ROW ");
            createTrigger.AppendLine();
            createTrigger.AppendLine();
            createTrigger.AppendLine($"Begin ");
            createTrigger.AppendLine($"\tUPDATE {this.MySqlObjectNames.TrackingTableQuotedShortName} ");
            createTrigger.AppendLine("\tSET `update_scope_id` = NULL ");
            createTrigger.AppendLine($"\t\t,`timestamp` = {MySqlObjectNames.TimestampValue}");
            createTrigger.AppendLine("\t\t,`last_change_datetime` = utc_timestamp()");

            createTrigger.Append($"\tWhere ");
            createTrigger.Append(MySqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.GetPrimaryKeysColumns(), this.MySqlObjectNames.TrackingTableQuotedShortName, "new"));

            createTrigger.AppendLine($"; ");

            createTrigger.AppendLine("IF (SELECT ROW_COUNT() = 0) THEN ");

            createTrigger.AppendLine($"\tINSERT INTO {this.MySqlObjectNames.TrackingTableQuotedShortName} (");

            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderArguments2 = new StringBuilder();
            StringBuilder stringPkAreNull = new StringBuilder();

            string argComma = string.Empty;
            string argAnd = string.Empty;
            foreach (var mutableColumn in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);

                stringBuilderArguments.AppendLine($"\t\t{argComma}{columnParser.QuotedShortName}");
                stringBuilderArguments2.AppendLine($"\t\t{argComma}new.{columnParser.QuotedShortName}");
                stringPkAreNull.Append($"{argAnd}{this.MySqlObjectNames.TrackingTableQuotedShortName}.{columnParser.QuotedShortName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            createTrigger.Append(stringBuilderArguments);
            createTrigger.AppendLine("\t\t,`update_scope_id`");
            createTrigger.AppendLine("\t\t,`timestamp`");
            createTrigger.AppendLine("\t\t,`sync_row_is_tombstone`");
            createTrigger.AppendLine("\t\t,`last_change_datetime`");

            var filterColumnsString = new StringBuilder();
            var filterColumnsString2 = new StringBuilder();
            var filterColumnsString3 = new StringBuilder();

            createTrigger.AppendLine("\t) ");
            createTrigger.AppendLine("\tVALUES (");
            createTrigger.Append(stringBuilderArguments2);
            createTrigger.AppendLine("\t\t,NULL");
            createTrigger.AppendLine($"\t\t,{MySqlObjectNames.TimestampValue}");
            createTrigger.AppendLine("\t\t,0");
            createTrigger.AppendLine("\t\t,utc_timestamp()");
            createTrigger.AppendLine("\t)");

            createTrigger.AppendLine("ON DUPLICATE KEY UPDATE");
            createTrigger.AppendLine("\t`update_scope_id` = NULL, ");
            createTrigger.AppendLine("\t`sync_row_is_tombstone` = 0, ");
            createTrigger.AppendLine($"\t`timestamp` = {MySqlObjectNames.TimestampValue}, ");
            createTrigger.AppendLine("\t`last_change_datetime` = utc_timestamp()");

            createTrigger.AppendLine(";");

            createTrigger.AppendLine("END IF;");

            createTrigger.AppendLine($"End; ");

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = createTrigger.ToString();

            return command;
        }

        /// <summary>
        /// Returns a command to check if a trigger exists.
        /// </summary>
        public Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {

            var triggerNameString = this.MySqlObjectNames.GetTriggerCommandName(triggerType);
            var triggerParser = new ObjectParser(triggerNameString, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = "select count(*) from information_schema.TRIGGERS where trigger_name = @triggerName AND trigger_schema = schema() limit 1";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@triggerName";
            parameter.Value = triggerParser.ObjectName;

            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }

        /// <summary>
        /// Returns the correct command.
        /// </summary>
        public Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction) => triggerType switch
        {
            DbTriggerType.Delete => Task.FromResult(this.CreateDeleteTriggerCommand(connection, transaction)),
            DbTriggerType.Insert => Task.FromResult(this.CreateInsertTriggerCommand(connection, transaction)),
            DbTriggerType.Update => Task.FromResult(this.CreateUpdateTriggerCommand(connection, transaction)),
            _ => throw new NotImplementedException(),
        };

        /// <summary>
        /// Returns a command to drop a trigger.
        /// </summary>
        public Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {
            var triggerNameString = this.MySqlObjectNames.GetTriggerCommandName(triggerType);

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = $"drop trigger {triggerNameString}";

            return Task.FromResult(command);
        }
    }
}