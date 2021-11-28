using Dotmim.Sync.Builders;
#if NET5_0 || NET6_0|| NETCOREAPP3_1
using MySqlConnector;
#elif NETSTANDARD 
using MySql.Data.MySqlClient;
#endif
using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

#if MARIADB
namespace Dotmim.Sync.MariaDB.Builders
#elif MYSQL
namespace Dotmim.Sync.MySql.Builders
#endif
{
    public class MySqlBuilderTrigger
    {
        private ParserName tableName;
        private ParserName trackingName;
        private SyncTable tableDescription;
        private SyncSetup setup;
        private string timestampValue;


        private MySqlObjectNames mySqlObjectNames;
        
        public MySqlBuilderTrigger(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
        {
            this.mySqlObjectNames = new MySqlObjectNames(tableDescription, tableName, trackingName, setup);
            this.timestampValue = MySqlObjectNames.TimestampValue;
            this.tableDescription = tableDescription;
            this.setup = setup;
            this.tableName = tableName;
            this.trackingName = trackingName;
        }

        public MySqlBuilderTrigger()
        {
        }


        public DbCommand CreateDeleteTriggerCommand(DbConnection connection, DbTransaction transaction)
        {
            var triggerName = this.mySqlObjectNames.GetTriggerCommandName(DbTriggerType.Delete);

            StringBuilder createTrigger = new StringBuilder();
            createTrigger.AppendLine($"CREATE TRIGGER {triggerName} AFTER DELETE ON {tableName.Quoted().ToString()} FOR EACH ROW ");
            createTrigger.AppendLine();
            createTrigger.AppendLine();
            createTrigger.AppendLine("BEGIN");

            createTrigger.AppendLine($"\tINSERT INTO {this.trackingName.Quoted().ToString()} (");

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

            createTrigger.Append(stringBuilderArguments.ToString());
            createTrigger.AppendLine("\t\t,`update_scope_id`");
            createTrigger.AppendLine("\t\t,`timestamp`");
            createTrigger.AppendLine("\t\t,`sync_row_is_tombstone`");
            createTrigger.AppendLine("\t\t,`last_change_datetime`");

            var filterColumnsString = new StringBuilder();
            var filterColumnsString2 = new StringBuilder();
            var filterColumnsString3 = new StringBuilder();



            createTrigger.AppendLine("\t) ");
            createTrigger.AppendLine("\tVALUES (");
            createTrigger.Append(stringBuilderArguments2.ToString());
            createTrigger.AppendLine("\t\t,NULL");
            createTrigger.AppendLine($"\t\t,{this.timestampValue}");
            createTrigger.AppendLine("\t\t,1");
            createTrigger.AppendLine("\t\t,utc_timestamp()");


            createTrigger.AppendLine("\t)");
            createTrigger.AppendLine("ON DUPLICATE KEY UPDATE");
            createTrigger.AppendLine("\t`update_scope_id` = NULL, ");
            createTrigger.AppendLine("\t`sync_row_is_tombstone` = 1, ");
            createTrigger.AppendLine($"\t`timestamp` = {this.timestampValue}, ");
            createTrigger.AppendLine("\t`last_change_datetime` = utc_timestamp()");

            createTrigger.Append(";");
            createTrigger.AppendLine("END");

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = createTrigger.ToString();

            return command;
        }


        public DbCommand CreateInsertTriggerCommand(DbConnection connection, DbTransaction transaction)
        {
            var insTriggerName = string.Format(this.mySqlObjectNames.GetTriggerCommandName(DbTriggerType.Insert), tableName.Unquoted().Normalized().ToString());

            StringBuilder createTrigger = new StringBuilder();
            createTrigger.AppendLine($"CREATE TRIGGER {insTriggerName} AFTER INSERT ON {tableName.Quoted().ToString()} FOR EACH ROW ");
            createTrigger.AppendLine();
            createTrigger.AppendLine();
            createTrigger.AppendLine("-- If row was deleted before, it already exists, so just make an update");
            createTrigger.AppendLine("BEGIN");

            createTrigger.AppendLine($"\tINSERT INTO {trackingName.Quoted().ToString()} (");

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

            createTrigger.Append(stringBuilderArguments.ToString());
            createTrigger.AppendLine("\t\t,`update_scope_id`");
            createTrigger.AppendLine("\t\t,`timestamp`");
            createTrigger.AppendLine("\t\t,`sync_row_is_tombstone`");
            createTrigger.AppendLine("\t\t,`last_change_datetime`");

            var filterColumnsString = new StringBuilder();
            var filterColumnsString2 = new StringBuilder();
            var filterColumnsString3 = new StringBuilder();

            createTrigger.AppendLine("\t) ");
            createTrigger.AppendLine("\tVALUES (");
            createTrigger.Append(stringBuilderArguments2.ToString());
            createTrigger.AppendLine("\t\t,NULL");
            createTrigger.AppendLine($"\t\t,{this.timestampValue}");
            createTrigger.AppendLine("\t\t,0");
            createTrigger.AppendLine("\t\t,utc_timestamp()");


            createTrigger.AppendLine("\t)");
            createTrigger.AppendLine("ON DUPLICATE KEY UPDATE");
            createTrigger.AppendLine("\t`update_scope_id` = NULL, ");
            createTrigger.AppendLine("\t`sync_row_is_tombstone` = 0, ");
            createTrigger.AppendLine($"\t`timestamp` = {this.timestampValue}, ");
            createTrigger.AppendLine("\t`last_change_datetime` = utc_timestamp()");

            createTrigger.Append(";");
            createTrigger.AppendLine("END");

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = createTrigger.ToString();

            return command;
        }

        public DbCommand CreateUpdateTriggerCommand(DbConnection connection, DbTransaction transaction)
        {
            var updTriggerName = string.Format(this.mySqlObjectNames.GetTriggerCommandName(DbTriggerType.Update), tableName.Unquoted().Normalized().ToString());
            StringBuilder createTrigger = new StringBuilder();
            createTrigger.AppendLine($"CREATE TRIGGER {updTriggerName} AFTER UPDATE ON {tableName.Quoted().ToString()} FOR EACH ROW ");
            createTrigger.AppendLine();
            createTrigger.AppendLine();
            createTrigger.AppendLine($"Begin ");
            createTrigger.AppendLine($"\tUPDATE {trackingName.Quoted().ToString()} ");
            createTrigger.AppendLine("\tSET `update_scope_id` = NULL ");
            createTrigger.AppendLine($"\t\t,`timestamp` = {this.timestampValue}");
            createTrigger.AppendLine("\t\t,`last_change_datetime` = utc_timestamp()");

            createTrigger.Append($"\tWhere ");
            createTrigger.Append(MySqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.GetPrimaryKeysColumns(), trackingName.Quoted().ToString(), "new"));

            if (this.tableDescription.GetMutableColumns().Count() > 0)
            {
                createTrigger.AppendLine();
                createTrigger.AppendLine("\t AND (");
                string or = "    ";
                foreach (var column in this.tableDescription.GetMutableColumns())
                {
                    var quotedColumn = ParserName.Parse(column, "`").Quoted().ToString();

                    createTrigger.Append("\t");
                    createTrigger.Append(or);
                    createTrigger.Append("IFNULL(");
                    createTrigger.Append("NULLIF(");
                    createTrigger.Append("`old`.");
                    createTrigger.Append(quotedColumn);
                    createTrigger.Append(", ");
                    createTrigger.Append("`new`.");
                    createTrigger.Append(quotedColumn);
                    createTrigger.Append(")");
                    createTrigger.Append(", ");
                    createTrigger.Append("NULLIF(");
                    createTrigger.Append("`new`.");
                    createTrigger.Append(quotedColumn);
                    createTrigger.Append(", ");
                    createTrigger.Append("`old`.");
                    createTrigger.Append(quotedColumn);
                    createTrigger.Append(")");
                    createTrigger.AppendLine(") IS NOT NULL");

                    or = " OR ";
                }
                createTrigger.AppendLine("\t ) ");
            }
            createTrigger.AppendLine($"; ");

            createTrigger.AppendLine("IF (SELECT ROW_COUNT() = 0) THEN ");

            createTrigger.AppendLine($"\tINSERT INTO {trackingName.Quoted().ToString()} (");

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

            createTrigger.Append(stringBuilderArguments.ToString());
            createTrigger.AppendLine("\t\t,`update_scope_id`");
            createTrigger.AppendLine("\t\t,`timestamp`");
            createTrigger.AppendLine("\t\t,`sync_row_is_tombstone`");
            createTrigger.AppendLine("\t\t,`last_change_datetime`");

            var filterColumnsString = new StringBuilder();
            var filterColumnsString2 = new StringBuilder();
            var filterColumnsString3 = new StringBuilder();

            createTrigger.AppendLine("\t) ");
            createTrigger.AppendLine("\tSELECT ");
            createTrigger.Append(stringBuilderArguments2.ToString());
            createTrigger.AppendLine("\t\t,NULL");
            createTrigger.AppendLine($"\t\t,{this.timestampValue}");
            createTrigger.AppendLine("\t\t,0");
            createTrigger.AppendLine("\t\t,utc_timestamp()");

            if (this.tableDescription.GetMutableColumns().Count() > 0)
            {
                createTrigger.AppendLine();
                createTrigger.AppendLine("\t WHERE (");
                string or = "    ";
                foreach (var column in this.tableDescription.GetMutableColumns())
                {
                    var quotedColumn = ParserName.Parse(column, "`").Quoted().ToString();

                    createTrigger.Append("\t");
                    createTrigger.Append(or);
                    createTrigger.Append("IFNULL(");
                    createTrigger.Append("NULLIF(");
                    createTrigger.Append("`old`.");
                    createTrigger.Append(quotedColumn);
                    createTrigger.Append(", ");
                    createTrigger.Append("`new`.");
                    createTrigger.Append(quotedColumn);
                    createTrigger.Append(")");
                    createTrigger.Append(", ");
                    createTrigger.Append("NULLIF(");
                    createTrigger.Append("`new`.");
                    createTrigger.Append(quotedColumn);
                    createTrigger.Append(", ");
                    createTrigger.Append("`old`.");
                    createTrigger.Append(quotedColumn);
                    createTrigger.Append(")");
                    createTrigger.AppendLine(") IS NOT NULL");

                    or = " OR ";
                }
                createTrigger.AppendLine("\t ) ");
            }
            createTrigger.AppendLine("ON DUPLICATE KEY UPDATE");
            createTrigger.AppendLine("\t`update_scope_id` = NULL, ");
            createTrigger.AppendLine("\t`sync_row_is_tombstone` = 0, ");
            createTrigger.AppendLine($"\t`timestamp` = {this.timestampValue}, ");
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

        public Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {

            var triggerNameString = string.Format(this.mySqlObjectNames.GetTriggerCommandName(triggerType), tableName.Unquoted().Normalized().ToString());
            var triggerName = ParserName.Parse(triggerNameString, "`");

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = "select count(*) from information_schema.TRIGGERS where trigger_name = @triggerName AND trigger_schema = schema() limit 1";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@triggerName";
            parameter.Value = triggerName.Unquoted().ToString();

            command.Parameters.Add(parameter);

            return Task.FromResult(command);

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

            var triggerNameString = string.Format(this.mySqlObjectNames.GetTriggerCommandName(triggerType), tableName.Unquoted().Normalized().ToString());

            var triggerName = ParserName.Parse(triggerNameString, "`");

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = $"drop trigger {triggerName.Unquoted().ToString()}";

            return Task.FromResult(command);
        }
    }
}
