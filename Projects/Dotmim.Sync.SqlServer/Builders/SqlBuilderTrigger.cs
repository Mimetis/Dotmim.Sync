using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;
using Dotmim.Sync.SqlServer.Manager;
using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer.Builders
{

    /// <summary>
    /// Sql trigger builder for Sql Server.
    /// </summary>
    public class SqlBuilderTrigger
    {
        private readonly SyncTable tableDescription;
        private readonly SqlObjectNames sqlObjectNames;
        private readonly SqlDbMetadata sqlDbMetadata;

        /// <inheritdoc cref="SqlBuilderTrigger" />
        public SqlBuilderTrigger(SyncTable tableDescription, SqlObjectNames sqlObjectNames, SqlDbMetadata sqlDbMetadata)
        {
            this.tableDescription = tableDescription;
            this.sqlObjectNames = sqlObjectNames;
            this.sqlDbMetadata = sqlDbMetadata;
        }

        /// <summary>
        /// Gets the command to check if a trigger exists.
        /// </summary>
        public virtual Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {

            var commandTriggerName = this.sqlObjectNames.GetTriggerCommandName(triggerType);
            var triggerNameParser = new ObjectParser(commandTriggerName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);

            var commandText = $@"IF EXISTS (SELECT tr.name FROM sys.triggers tr  
                                            JOIN sys.tables t ON tr.parent_id = t.object_id 
                                            JOIN sys.schemas s ON t.schema_id = s.schema_id 
                                            WHERE tr.name = @triggerName and s.name = @schemaName) SELECT 1 ELSE SELECT 0";

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = commandText;

            var p1 = command.CreateParameter();
            p1.ParameterName = "@triggerName";
            p1.Value = triggerNameParser.ObjectName;
            command.Parameters.Add(p1);

            var p2 = command.CreateParameter();
            p2.ParameterName = "@schemaName";
            p2.Value = SqlManagementUtils.GetUnquotedSqlSchemaName(triggerNameParser);
            command.Parameters.Add(p2);

            return Task.FromResult(command);
        }

        /// <summary>
        /// Gets the command to drop a trigger.
        /// </summary>
        public virtual Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {

            var commandTriggerName = this.sqlObjectNames.GetTriggerCommandName(triggerType);

            var commandText = $@"DROP TRIGGER {commandTriggerName}";

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = commandText;

            return Task.FromResult(command);
        }

        /// <summary>
        /// Gets the command to create a trigger.
        /// </summary>
        public virtual Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {

            var commandTriggerCommandString = triggerType switch
            {
                DbTriggerType.Delete => this.CreateDeleteTriggerAsync(),
                DbTriggerType.Insert => this.CreateInsertTriggerAsync(),
                DbTriggerType.Update => this.CreateUpdateTriggerAsync(),
                _ => throw new NotImplementedException(),
            };
            string triggerFor = triggerType == DbTriggerType.Delete ? "DELETE"
                              : triggerType == DbTriggerType.Update ? "UPDATE"
                              : "INSERT";

            var commandTriggerName = this.sqlObjectNames.GetTriggerCommandName(triggerType);

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE TRIGGER {commandTriggerName} ON {this.sqlObjectNames.TableQuotedFullName} FOR {triggerFor} AS");
            stringBuilder.AppendLine(commandTriggerCommandString);

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = stringBuilder.ToString();

            return Task.FromResult(command);
        }

        private string CreateDeleteTriggerAsync()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("SET NOCOUNT ON;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("UPDATE [side] ");
            stringBuilder.AppendLine("SET  [sync_row_is_tombstone] = 1");
            stringBuilder.AppendLine("\t,[update_scope_id] = NULL -- scope id is always NULL when update is made locally");
            stringBuilder.AppendLine("\t,[last_change_datetime] = GetUtcDate()");
            stringBuilder.AppendLine($"FROM {this.sqlObjectNames.TrackingTableQuotedFullName} [side]");
            stringBuilder.Append($"JOIN DELETED AS [d] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[side]", "[d]"));
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"INSERT INTO {this.sqlObjectNames.TrackingTableQuotedFullName} (");

            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();

            string argComma = " ";
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);

                stringBuilderArguments.AppendLine($"\t{argComma}[d].{columnParser.QuotedShortName}");
                stringBuilderArguments2.AppendLine($"\t{argComma}{columnParser.QuotedShortName}");
                stringPkAreNull.Append($"{argAnd}[side].{columnParser.QuotedShortName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(stringBuilderArguments2);
            stringBuilder.AppendLine("\t,[update_scope_id]");
            stringBuilder.AppendLine("\t,[sync_row_is_tombstone]");
            stringBuilder.AppendLine("\t,[last_change_datetime]");
            stringBuilder.AppendLine(") ");
            stringBuilder.AppendLine("SELECT");
            stringBuilder.Append(stringBuilderArguments);
            stringBuilder.AppendLine("\t,NULL");
            stringBuilder.AppendLine("\t,1");
            stringBuilder.AppendLine("\t,GetUtcDate()");
            stringBuilder.AppendLine("FROM DELETED [d]");
            stringBuilder.Append($"LEFT JOIN {this.sqlObjectNames.TrackingTableQuotedFullName} [side] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[d]", "[side]"));
            stringBuilder.Append("WHERE ");
            stringBuilder.AppendLine(stringPkAreNull.ToString());

            return stringBuilder.ToString();
        }

        private string CreateInsertTriggerAsync()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("SET NOCOUNT ON;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- If row was deleted before, it already exists, so just make an update");
            stringBuilder.AppendLine("UPDATE [side] ");
            stringBuilder.AppendLine("SET  [sync_row_is_tombstone] = 0");
            stringBuilder.AppendLine("\t,[update_scope_id] = NULL -- scope id is always NULL when update is made locally");
            stringBuilder.AppendLine("\t,[last_change_datetime] = GetUtcDate()");
            stringBuilder.AppendLine($"FROM {this.sqlObjectNames.TrackingTableQuotedFullName} [side]");
            stringBuilder.Append($"JOIN INSERTED AS [i] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[side]", "[i]"));
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"INSERT INTO {this.sqlObjectNames.TrackingTableQuotedFullName} (");

            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();

            string argComma = " ";
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
                stringBuilderArguments.AppendLine($"\t{argComma}[i].{columnParser.QuotedShortName}");
                stringBuilderArguments2.AppendLine($"\t{argComma}{columnParser.QuotedShortName}");
                stringPkAreNull.Append($"{argAnd}[side].{columnParser.QuotedShortName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(stringBuilderArguments2);
            stringBuilder.AppendLine("\t,[update_scope_id]");
            stringBuilder.AppendLine("\t,[sync_row_is_tombstone]");
            stringBuilder.AppendLine("\t,[last_change_datetime]");
            stringBuilder.AppendLine(") ");
            stringBuilder.AppendLine("SELECT");
            stringBuilder.Append(stringBuilderArguments);
            stringBuilder.AppendLine("\t,NULL");
            stringBuilder.AppendLine("\t,0");
            stringBuilder.AppendLine("\t,GetUtcDate()");
            stringBuilder.AppendLine("FROM INSERTED [i]");
            stringBuilder.Append($"LEFT JOIN {this.sqlObjectNames.TrackingTableQuotedFullName} [side] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[i]", "[side]"));
            stringBuilder.Append("WHERE ");
            stringBuilder.AppendLine(stringPkAreNull.ToString());

            return stringBuilder.ToString();
        }

        private string CreateUpdateTriggerAsync()
        {

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("SET NOCOUNT ON;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("UPDATE [side] ");
            stringBuilder.AppendLine("SET \t[update_scope_id] = NULL -- since the update if from local, it's a NULL");
            stringBuilder.AppendLine("\t,[last_change_datetime] = GetUtcDate()");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"FROM {this.sqlObjectNames.TrackingTableQuotedFullName} [side]");
            stringBuilder.Append($"JOIN INSERTED AS [i] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[side]", "[i]"));

            stringBuilder.AppendLine($"INSERT INTO {this.sqlObjectNames.TrackingTableQuotedFullName} (");

            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();

            string argComma = " ";
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
                stringBuilderArguments.AppendLine($"\t{argComma}[i].{columnParser.QuotedShortName}");
                stringBuilderArguments2.AppendLine($"\t{argComma}{columnParser.QuotedShortName}");
                stringPkAreNull.Append($"{argAnd}[side].{columnParser.QuotedShortName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(stringBuilderArguments2);
            stringBuilder.AppendLine("\t,[update_scope_id]");
            stringBuilder.AppendLine("\t,[sync_row_is_tombstone]");
            stringBuilder.AppendLine("\t,[last_change_datetime]");
            stringBuilder.AppendLine(") ");
            stringBuilder.AppendLine("SELECT");
            stringBuilder.Append(stringBuilderArguments);
            stringBuilder.AppendLine("\t,NULL");
            stringBuilder.AppendLine("\t,0");
            stringBuilder.AppendLine("\t,GetUtcDate()");
            stringBuilder.AppendLine("FROM INSERTED [i]");
            stringBuilder.Append($"JOIN DELETED AS [d] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[d]", "[i]"));
            stringBuilder.Append($"LEFT JOIN {this.sqlObjectNames.TrackingTableQuotedFullName} [side] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[i]", "[side]"));
            stringBuilder.Append("WHERE ");
            stringBuilder.AppendLine(stringPkAreNull.ToString());

            return stringBuilder.ToString();
        }
    }
}