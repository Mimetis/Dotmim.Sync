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
        private NpgsqlObjectNames objectNames;
        private string scopeName;
        private SyncSetup setup;
        private SyncTable tableDescription;
        private ParserName tableName;
        private ParserName trackingName;
        public NpgsqlBuilderTrigger(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
        {
            this.tableDescription = tableDescription;
            this.tableName = tableName;
            this.trackingName = trackingTableName;
            this.setup = setup;
            this.scopeName = scopeName;
            this.objectNames = new NpgsqlObjectNames(this.tableDescription, tableName, trackingTableName, this.setup, scopeName);
        }
        public virtual Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {
            var commandTriggerName = this.objectNames.GetTriggerCommandName(triggerType);
            var triggerName = ParserName.Parse(commandTriggerName).ToString();

            var commandText = $@"
                                SELECT EXISTS
	                                (SELECT
		                                FROM INFORMATION_SCHEMA.TRIGGERS
		                                WHERE TRIGGER_SCHEMA = @schemaName
			                                AND TRIGGER_NAME = @triggerName )";

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
            p2.Value = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(ParserName.Parse(commandTriggerName));
            command.Parameters.Add(p2);

            return Task.FromResult(command);

        }
        public virtual Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {

            var commandTriggerName = this.objectNames.GetTriggerCommandName(triggerType);

            var commandText = $@"drop trigger IF EXISTS {commandTriggerName} on {tableName.Schema().Unquoted().ToString()}";

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

            var commandTriggerName = this.objectNames.GetTriggerCommandName(triggerType);

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine(commandTriggerCommandString);
            stringBuilder.AppendLine($@"
                                  CREATE OR REPLACE TRIGGER {commandTriggerName}
                                  AFTER {triggerFor}
                                  ON {tableName.Schema().Unquoted().ToString()}
                                  FOR EACH ROW
                                  EXECUTE FUNCTION {tableName.Unquoted().SchemaName}.{commandTriggerName}_function()");

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = stringBuilder.ToString();

            return Task.FromResult(command);
        }


        private string CreateInsertTriggerAsync()
        {
            var tablename = tableName.Unquoted().ToString();
            var stringBuilder = new StringBuilder(
                    @$"
                        CREATE OR REPLACE FUNCTION {tableName.Schema().Unquoted().ToString()}_insert_trigger_function()
                            RETURNS trigger
                            LANGUAGE 'plpgsql'
                            COST 100
                            VOLATILE NOT LEAKPROOF
                        AS $BODY$
                        BEGIN
	                        UPDATE side 
	                        SET  sync_row_is_tombstone = 0
		                        ,update_scope_id = NULL -- scope id is always NULL when update is made locally
		                        ,last_change_datetime = GetUtcDate()
	                        FROM {trackingName.Schema().Unquoted().ToString()} side
                            JOIN NEW AS i ON ");
            stringBuilder.AppendLine(NpgsqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "side", "i")).Append(";");
            stringBuilder.Append(@$"INSERT INTO {trackingName.Schema().Unquoted().ToString()} ( ");
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();

            string argComma = " ";
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Unquoted().ToString();
                stringBuilderArguments.AppendLine($"\t{argComma}i.{columnName}");
                stringBuilderArguments2.AppendLine($"\t{argComma}{columnName}");
                stringPkAreNull.Append($"{argAnd}side.{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }
            stringBuilder.Append(stringBuilderArguments2.ToString());
            stringBuilder.Append(@",update_scope_id
		                        ,timestamp
		                        ,sync_row_is_tombstone
		                        ,last_change_datetime
	                        ) 
	                        SELECT");
            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.Append(@$",NULL
		                        ,0
		                        ,(extract(epoch from now()) * 1000)
		                        ,GetUtcDate()
	                        FROM NEW AS i
	                        LEFT JOIN {trackingName.Schema().Unquoted().ToString()} side ON ");
            stringBuilder.AppendLine(NpgsqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "side", "i"));
            stringBuilder.AppendLine("WHERE ");
            stringBuilder.AppendLine(stringPkAreNull.ToString()).Append(";");
            stringBuilder.Append(@"END;
                        $BODY$;");

            return stringBuilder.ToString();
        }
        private string CreateUpdateTriggerAsync()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($@"
                        CREATE OR REPLACE FUNCTION {tableName.Schema().Unquoted().ToString()}_update_trigger_function()
                            RETURNS trigger
                            LANGUAGE 'plpgsql'
                            COST 100
                            VOLATILE NOT LEAKPROOF
                        AS $BODY$
                        BEGIN
	                        UPDATE ""side"" 
	                        SET  ""sync_row_is_tombstone"" = 0
		                        ,""update_scope_id"" = NULL -- scope id is always NULL when update is made locally
		                        ,""last_change_datetime"" = GetUtcDate()
	                        FROM {tableName.Schema().Unquoted().ToString()} ""side""
	                        JOIN NEW AS ""i"" ON ""side"".""ProductID"" = ""i"".""ProductID"";

	                        INSERT INTO {tableName.Schema().Unquoted().ToString()} (
		                            ""ProductID""
		                        ,""update_scope_id""
		                        ,""sync_row_is_tombstone""
		                        ,""last_change_datetime""
	                        ) 
	                        SELECT
		                            ""i"".""ProductID""
		                        ,NULL
		                        ,0
		                        ,GetUtcDate()
	                        FROM NEW AS ""i""
	                        LEFT JOIN {tableName.Schema().Unquoted().ToString()} ""side"" ON ""i"".""ProductID"" = ""side"".""ProductID""
	                        WHERE ""side"".""ProductID"" IS NULL;
                        END;
                        $BODY$;
");
            return stringBuilder.ToString();

        }
        private string CreateDeleteTriggerAsync()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($@"
                                CREATE OR REPLACE FUNCTION {tableName.Schema().Unquoted().ToString()}_delete_trigger_function()
                                    RETURNS trigger
                                    LANGUAGE 'plpgsql'
                                    COST 100
                                    VOLATILE NOT LEAKPROOF
                                AS $BODY$
                                BEGIN
                                INSERT INTO {trackingName.Schema().Unquoted().ToString()} (
	                                 ""AddressID""
	                                ,update_scope_id
	                                ,sync_row_is_tombstone
	                                ,last_change_datetime
	                                ,timestamp
                                ) 
                                VALUES (
	                                 OLD.""AddressID""
	                                ,NULL
	                                ,true
	                                ,now()
	                                ,to_char(current_timestamp, 'YYYYDDDSSSSUS')::bigint
                                )
                                ON CONFLICT( ""AddressID"")
                                DO UPDATE SET
	                                sync_row_is_tombstone = true
	                                ,update_scope_id = NULL
	                                ,last_change_datetime = now()
	                                ,timestamp = to_char(current_timestamp, 'YYYYDDDSSSSUS')::bigint;

                                RETURN NULL;
                                END;
                                $BODY$;
");
            return stringBuilder.ToString();
        }
    }
}