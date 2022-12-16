using Dotmim.Sync.Builders;



using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Globalization;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlBuilderTrigger
    {
        private ParserName tableName;
        private ParserName trackingName;
        private readonly SyncTable tableDescription;
        private readonly SyncSetup setup;
        private readonly SqlObjectNames sqlObjectNames;
        public SqlBuilderTrigger(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup, string scopeName)
        {
            this.tableDescription = tableDescription;
            this.setup = setup;
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.sqlObjectNames = new SqlObjectNames(this.tableDescription, tableName, trackingName, this.setup, scopeName);

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
            stringBuilder.AppendLine($"FROM {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.Append($"JOIN DELETED AS [d] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[side]", "[d]"));
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"INSERT INTO {trackingName.Schema().Quoted().ToString()} (");

            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();

            string argComma = " ";
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilderArguments.AppendLine($"\t{argComma}[d].{columnName}");
                stringBuilderArguments2.AppendLine($"\t{argComma}{columnName}");
                stringPkAreNull.Append($"{argAnd}[side].{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(stringBuilderArguments2.ToString());
            stringBuilder.AppendLine("\t,[update_scope_id]");
            stringBuilder.AppendLine("\t,[sync_row_is_tombstone]");
            stringBuilder.AppendLine("\t,[last_change_datetime]");
            stringBuilder.AppendLine(") ");
            stringBuilder.AppendLine("SELECT");
            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine("\t,NULL");
            stringBuilder.AppendLine("\t,1");
            stringBuilder.AppendLine("\t,GetUtcDate()");
            stringBuilder.AppendLine("FROM DELETED [d]");
            stringBuilder.Append($"LEFT JOIN {trackingName.Schema().Quoted().ToString()} [side] ON ");
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
            stringBuilder.AppendLine($"FROM {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.Append($"JOIN INSERTED AS [i] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[side]", "[i]"));
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"INSERT INTO {trackingName.Schema().Quoted().ToString()} (");

            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();

            string argComma = " ";
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilderArguments.AppendLine($"\t{argComma}[i].{columnName}");
                stringBuilderArguments2.AppendLine($"\t{argComma}{columnName}");
                stringPkAreNull.Append($"{argAnd}[side].{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(stringBuilderArguments2.ToString());
            stringBuilder.AppendLine("\t,[update_scope_id]");
            stringBuilder.AppendLine("\t,[sync_row_is_tombstone]");
            stringBuilder.AppendLine("\t,[last_change_datetime]");
            stringBuilder.AppendLine(") ");
            stringBuilder.AppendLine("SELECT");
            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine("\t,NULL");
            stringBuilder.AppendLine("\t,0");
            stringBuilder.AppendLine("\t,GetUtcDate()");
            stringBuilder.AppendLine("FROM INSERTED [i]");
            stringBuilder.Append($"LEFT JOIN {trackingName.Schema().Quoted().ToString()} [side] ON ");
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

            stringBuilder.AppendLine($"FROM {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.Append($"JOIN INSERTED AS [i] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[side]", "[i]"));

            //if (this.tableDescription.GetMutableColumns().Count() > 0)
            //{
            //    stringBuilder.Append($"JOIN DELETED AS [d] ON ");
            //    stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[d]", "[i]"));

            //    stringBuilder.AppendLine("WHERE (");
            //    string or = "";
            //    foreach (var column in this.tableDescription.GetMutableColumns())
            //    {
            //        var quotedColumn = ParserName.Parse(column).Quoted().ToString();

            //        stringBuilder.Append("\t");
            //        stringBuilder.Append(or);
            //        stringBuilder.Append("ISNULL(");
            //        stringBuilder.Append("NULLIF(");
            //        stringBuilder.Append("[d].");
            //        stringBuilder.Append(quotedColumn);
            //        stringBuilder.Append(", ");
            //        stringBuilder.Append("[i].");
            //        stringBuilder.Append(quotedColumn);
            //        stringBuilder.Append(")");
            //        stringBuilder.Append(", ");
            //        stringBuilder.Append("NULLIF(");
            //        stringBuilder.Append("[i].");
            //        stringBuilder.Append(quotedColumn);
            //        stringBuilder.Append(", ");
            //        stringBuilder.Append("[d].");
            //        stringBuilder.Append(quotedColumn);
            //        stringBuilder.Append(")");
            //        stringBuilder.AppendLine(") IS NOT NULL");

            //        or = " OR ";
            //    }
            //    stringBuilder.AppendLine(") ");
            //}

            stringBuilder.AppendLine($"INSERT INTO {trackingName.Schema().Quoted().ToString()} (");

            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();

            string argComma = " ";
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilderArguments.AppendLine($"\t{argComma}[i].{columnName}");
                stringBuilderArguments2.AppendLine($"\t{argComma}{columnName}");
                stringPkAreNull.Append($"{argAnd}[side].{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(stringBuilderArguments2.ToString());
            stringBuilder.AppendLine("\t,[update_scope_id]");
            stringBuilder.AppendLine("\t,[sync_row_is_tombstone]");
            stringBuilder.AppendLine("\t,[last_change_datetime]");
            stringBuilder.AppendLine(") ");
            stringBuilder.AppendLine("SELECT");
            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine("\t,NULL");
            stringBuilder.AppendLine("\t,0");
            stringBuilder.AppendLine("\t,GetUtcDate()");
            stringBuilder.AppendLine("FROM INSERTED [i]");
            stringBuilder.Append($"JOIN DELETED AS [d] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[d]", "[i]"));
            stringBuilder.Append($"LEFT JOIN {trackingName.Schema().Quoted().ToString()} [side] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[i]", "[side]"));
            stringBuilder.Append("WHERE ");
            stringBuilder.AppendLine(stringPkAreNull.ToString());

            //if (this.tableDescription.GetMutableColumns().Count() > 0)
            //{
            //    stringBuilder.AppendLine("AND (");
            //    string or = "";
            //    foreach (var column in this.tableDescription.GetMutableColumns())
            //    {
            //        var quotedColumn = ParserName.Parse(column).Quoted().ToString();

            //        stringBuilder.Append("\t");
            //        stringBuilder.Append(or);
            //        stringBuilder.Append("ISNULL(");
            //        stringBuilder.Append("NULLIF(");
            //        stringBuilder.Append("[d].");
            //        stringBuilder.Append(quotedColumn);
            //        stringBuilder.Append(", ");
            //        stringBuilder.Append("[i].");
            //        stringBuilder.Append(quotedColumn);
            //        stringBuilder.Append(")");
            //        stringBuilder.Append(", ");
            //        stringBuilder.Append("NULLIF(");
            //        stringBuilder.Append("[i].");
            //        stringBuilder.Append(quotedColumn);
            //        stringBuilder.Append(", ");
            //        stringBuilder.Append("[d].");
            //        stringBuilder.Append(quotedColumn);
            //        stringBuilder.Append(")");
            //        stringBuilder.AppendLine(") IS NOT NULL");

            //        or = " OR ";
            //    }
            //    stringBuilder.AppendLine(") ");
            //}

            return stringBuilder.ToString();

        }

        public virtual Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {

            var commandTriggerName = this.sqlObjectNames.GetTriggerCommandName(triggerType);
            var triggerName = ParserName.Parse(commandTriggerName).ToString();
            var schemaNameString = string.IsNullOrEmpty(ParserName.Parse(commandTriggerName).SchemaName) ? DBNull.Value : (object)ParserName.Parse(commandTriggerName).SchemaName;


            var commandText = $@"IF EXISTS (SELECT tr.name FROM sys.triggers tr  
                                            JOIN sys.tables t ON tr.parent_id = t.object_id 
                                            JOIN sys.schemas s ON t.schema_id = s.schema_id 
                                            WHERE tr.name = @triggerName and (s.name = @schemaName or @schemaName is null)) SELECT 1 ELSE SELECT 0";

            var command = connection.CreateCommand();
            command.Connection = connection;
                command.Transaction = transaction;

            command.CommandText = commandText;

            var p1 = command.CreateParameter();
            p1.ParameterName = "@triggerName";
            p1.Value = triggerName;
            command.Parameters.Add(p1);

            var p2 = command.CreateParameter();
            p2.ParameterName = "@schemaName";
            p2.Value = schemaNameString;
            command.Parameters.Add(p2);

            return Task.FromResult(command);

        }
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
        public virtual Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {

            var commandTriggerCommandString = triggerType switch
            {
                DbTriggerType.Delete => CreateDeleteTriggerAsync(),
                DbTriggerType.Insert => CreateInsertTriggerAsync(),
                DbTriggerType.Update => CreateUpdateTriggerAsync(),
                _ => throw new NotImplementedException()
            };
            string triggerFor = triggerType == DbTriggerType.Delete ? "DELETE"
                              : triggerType == DbTriggerType.Update ? "UPDATE"
                              : "INSERT";

            var commandTriggerName = this.sqlObjectNames.GetTriggerCommandName(triggerType);

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE TRIGGER {commandTriggerName} ON {tableName.Schema().Quoted().ToString()} FOR {triggerFor} AS");
            stringBuilder.AppendLine(commandTriggerCommandString);

            var command = connection.CreateCommand();
            command.Connection = connection;
                command.Transaction = transaction;

            command.CommandText = stringBuilder.ToString();

            return Task.FromResult(command);
        }

    }
}