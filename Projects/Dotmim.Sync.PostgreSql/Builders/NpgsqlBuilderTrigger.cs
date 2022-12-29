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
        private string timestampValue;

        public NpgsqlBuilderTrigger(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
        {
            this.tableDescription = tableDescription;
            this.tableName = tableName;
            this.trackingName = trackingTableName;
            this.setup = setup;
            this.scopeName = scopeName;
            this.objectNames = new NpgsqlObjectNames(this.tableDescription, tableName, trackingTableName, this.setup, scopeName);
            this.timestampValue = NpgsqlObjectNames.TimestampValue;
        }
        public virtual Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {
            var commandTriggerName = this.objectNames.GetTriggerCommandName(triggerType);
            var triggerName = ParserName.Parse(commandTriggerName).ToString();

            var commandText = $@"
                                select exists
	                                (select
		                                from information_schema.triggers
		                                where trigger_schema = @schemaname
			                                and trigger_name = @triggername )";

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = commandText;

            var p1 = command.CreateParameter();
            p1.ParameterName = "@triggername";
            p1.Value = triggerName;
            command.Parameters.Add(p1);

            var p2 = command.CreateParameter();
            p2.ParameterName = "@schemaname";
            p2.Value = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(ParserName.Parse(commandTriggerName));
            command.Parameters.Add(p2);

            return Task.FromResult(command);

        }
        public virtual Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {
            var commandTriggerName = this.objectNames.GetTriggerCommandName(triggerType);
            var commandTriggerNameQuoted = ParserName.Parse(commandTriggerName, "\"").Quoted().ToString();
            var tableQuoted = ParserName.Parse(tableName.ToString(), "\"").Quoted().ToString();
            var tableUnquoted = tableName.Unquoted().ToString();
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);

            var commandText = $@"drop trigger if exists {commandTriggerNameQuoted} on {schema}.{tableQuoted}";

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = commandText;

            return Task.FromResult(command);
        }
        public virtual Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {
            var commandTriggerName = this.objectNames.GetTriggerCommandName(triggerType);
            var commandTriggerNameQuoted = ParserName.Parse(commandTriggerName, "\"").Quoted().ToString();

            var commandTriggerFunctionString = triggerType switch

            {
                DbTriggerType.Insert => CreateInsertTriggerAsync(commandTriggerName),
                DbTriggerType.Update => CreateUpdateTriggerAsync(commandTriggerName),
                DbTriggerType.Delete => CreateDeleteTriggerAsync(commandTriggerName),
                _ => throw new NotImplementedException()
            };
            string triggerFor = triggerType == DbTriggerType.Delete ? "DELETE"
                              : triggerType == DbTriggerType.Update ? "UPDATE"
                              : "INSERT";

            var tableQuoted = ParserName.Parse(tableName.ToString(), "\"").Quoted().ToString();
            var tableUnquoted = tableName.Unquoted().ToString();
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine(commandTriggerFunctionString);
            stringBuilder.AppendLine($@"
                                  create or replace trigger {commandTriggerNameQuoted}
                                  AFTER {triggerFor}
                                  ON {schema}.{tableQuoted}
                                  FOR EACH ROW
                                  EXECUTE FUNCTION {schema}.{commandTriggerName}_function()");

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = stringBuilder.ToString();

            return Task.FromResult(command);
        }

        private string CreateInsertTriggerAsync(string triggerName)
        {
            var trackingTableQuoted = ParserName.Parse(trackingName.ToString(), "\"").Quoted().ToString();
            var tableQuoted = ParserName.Parse(tableName.ToString(), "\"").Quoted().ToString();
            var tableUnquoted = tableName.Unquoted().ToString();
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);


            var tablename = tableName.Quoted().ToString();
            var idColumnsSelects = new StringBuilder();
            var idColumns = new StringBuilder();
            //var stringPkAreNull = new StringBuilder();
            string argComma = " ";
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                idColumns.AppendLine($"{argComma}{columnName}");
                idColumnsSelects.AppendLine($"{argComma}NEW.{columnName}");
                //stringPkAreNull.Append($"{argAnd}side.{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }
            string stringBuilder = @$"CREATE OR REPLACE FUNCTION  {schema}.{triggerName.ToLower()}_function()
                                    RETURNS trigger
                                    LANGUAGE 'plpgsql'
                                    COST 100
                                    VOLATILE NOT LEAKPROOF
                                AS $new$
                                BEGIN
                                    insert into {schema}.{trackingTableQuoted} 
                                    ({idColumns.ToString()}
                                        , ""update_scope_id""
                                        ,""timestamp""
                                        ,""sync_row_is_tombstone""
                                        ,""last_change_datetime""
                                        )
                                    values( {idColumnsSelects.ToString()}
                                        ,null
                                        ,{this.timestampValue}
                                        ,FALSE
                                        ,now()
                                        )
                                    on conflict({idColumns.ToString()}) do update
                                   SET
                                   ""timestamp"" = {this.timestampValue}
                                    ,""sync_row_is_tombstone"" = FALSE
                                    ,""update_scope_id"" = null
                                    ,""last_change_datetime"" = now();
                                return NEW;
                                end;
                                $new$;";
            var query = stringBuilder.ToString();
            return stringBuilder;
        }
        private string CreateUpdateTriggerAsync(string triggerName)
        {
            var trackingTableQuoted = ParserName.Parse(trackingName.ToString(), "\"").Quoted().ToString();
            var tableQuoted = ParserName.Parse(tableName.ToString(), "\"").Quoted().ToString();
            var tableUnquoted = tableName.Unquoted().ToString();
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);

            var tablename = tableName.Quoted().ToString();
            var idColumnsSelects = new StringBuilder();
            var idColumns = new StringBuilder();
            //var stringPkAreNull = new StringBuilder();
            string argComma = " ";
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                idColumns.AppendLine($"{argComma}{columnName}");
                idColumnsSelects.AppendLine($"{argComma}NEW.{columnName}");
                //stringPkAreNull.Append($"{argAnd}side.{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }
            string stringBuilder = @$"CREATE OR REPLACE FUNCTION {schema}.{triggerName.ToLower()}_function()
                                    RETURNS trigger
                                    LANGUAGE 'plpgsql'
                                    COST 100
                                    VOLATILE NOT LEAKPROOF
                                AS $new$
                                BEGIN
                                    insert into {schema}.{trackingTableQuoted} 
                                    ({idColumns.ToString()}
                                        , ""update_scope_id""
                                        ,""timestamp""
                                        ,""sync_row_is_tombstone""
                                        ,""last_change_datetime""
                                        )
                                    values( {idColumnsSelects.ToString()}
                                        ,null
                                        ,{this.timestampValue}
                                        ,FALSE
                                        ,now()
                                        )
                                    on conflict({idColumns.ToString()}) do update
                                   SET
                                   ""timestamp"" = {this.timestampValue}
                                    ,""sync_row_is_tombstone"" = FALSE
                                    ,""update_scope_id"" = null
                                    ,""last_change_datetime"" = now();
                                return NEW;
                                end;
                                $new$;";
            var query = stringBuilder.ToString();
            return stringBuilder;
        }
        private string CreateDeleteTriggerAsync(string triggerName)
        {
            var trackingTableQuoted = ParserName.Parse(trackingName.ToString(), "\"").Quoted().ToString();
            var tableQuoted = ParserName.Parse(tableName.ToString(), "\"").Quoted().ToString();
            var tableUnquoted = tableName.Unquoted().ToString();
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);


            var tablename = tableName.Quoted().ToString();
            var idColumnsSelects = new StringBuilder();
            var idColumns = new StringBuilder();
            //var stringPkAreNull = new StringBuilder();
            string argComma = " ";
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                idColumns.AppendLine($"{argComma}{columnName}");
                idColumnsSelects.AppendLine($"{argComma}OLD.{columnName}");
                //stringPkAreNull.Append($"{argAnd}side.{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }
            string stringBuilder = @$"CREATE OR REPLACE FUNCTION {schema}.{triggerName.ToLower()}_function()
                                    RETURNS trigger
                                    LANGUAGE 'plpgsql'
                                    COST 100
                                    VOLATILE NOT LEAKPROOF
                                AS $new$
                                BEGIN
                                    insert into {schema}.{trackingTableQuoted} 
                                    ({idColumns.ToString()}
                                        , ""update_scope_id""
                                        ,""timestamp""
                                        ,""sync_row_is_tombstone""
                                        ,""last_change_datetime""
                                        )
                                    values( {idColumnsSelects.ToString()}
                                        ,null
                                        ,{this.timestampValue}
                                        ,TRUE
                                        ,now()
                                        )
                                    on conflict({idColumns.ToString()}) do update
                                   SET
                                   ""timestamp"" = {this.timestampValue}
                                    ,""sync_row_is_tombstone"" = TRUE
                                    ,""update_scope_id"" = null
                                    ,""last_change_datetime"" = now();
                                return OLD;
                                end;
                                $new$;";
            var query = stringBuilder.ToString();
            return stringBuilder;
        }
    }
}