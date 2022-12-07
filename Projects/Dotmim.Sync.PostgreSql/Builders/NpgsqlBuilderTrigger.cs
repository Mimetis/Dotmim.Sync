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
            var commandTriggerFunctionString = triggerType switch

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
            stringBuilder.AppendLine(commandTriggerFunctionString);
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
            var idColumnsSelects = new StringBuilder();
            var idColumns = new StringBuilder();
            //var stringPkAreNull = new StringBuilder();
            string argComma = " ";
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Unquoted().ToString();
                idColumns.AppendLine($"{argComma}{columnName}");
                idColumnsSelects.AppendLine($"{argComma}NEW.{columnName}");
                //stringPkAreNull.Append($"{argAnd}side.{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }
            string stringBuilder = @$"CREATE OR REPLACE FUNCTION {tableName.Schema().Unquoted().ToString()}_insert_trigger_function()
                                    RETURNS trigger
                                    LANGUAGE 'plpgsql'
                                    COST 100
                                    VOLATILE NOT LEAKPROOF
                                AS $new$
                                BEGIN
                                    insert into {trackingName.Schema().Unquoted().ToString()} 
                                    ({idColumns.ToString()}
                                        , update_scope_id
                                        ,""timestamp""
                                        ,sync_row_is_tombstone
                                        ,last_change_datetime
                                        )
                                    values( {idColumnsSelects.ToString()}
                                        ,null
                                        ,{this.timestampValue}
                                        ,0::bit
                                        ,now()
                                        )
                                    on conflict({idColumns.ToString()}) do update
                                   SET
                                   ""timestamp"" = {this.timestampValue}
                                    ,sync_row_is_tombstone = 0::bit
                                    ,update_scope_id = null
                                    ,last_change_datetime = now();
                                return NEW;
                                end;
                                $new$;";
            
            return stringBuilder;
        }
        private string CreateUpdateTriggerAsync()
        {
            var tablename = tableName.Unquoted().ToString();
            var idColumnsSelects = new StringBuilder();
            var idColumns = new StringBuilder();
            //var stringPkAreNull = new StringBuilder();
            string argComma = " ";
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Unquoted().ToString();
                idColumns.AppendLine($"{argComma}{columnName}");
                idColumnsSelects.AppendLine($"{argComma}NEW.{columnName}");
                //stringPkAreNull.Append($"{argAnd}side.{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }
            string stringBuilder = @$"CREATE OR REPLACE FUNCTION {tableName.Schema().Unquoted().ToString()}_update_trigger_function()
                                    RETURNS trigger
                                    LANGUAGE 'plpgsql'
                                    COST 100
                                    VOLATILE NOT LEAKPROOF
                                AS $new$
                                BEGIN
                                    insert into {trackingName.Schema().Unquoted().ToString()} 
                                    ({idColumns.ToString()}
                                        , update_scope_id
                                        ,""timestamp""
                                        ,sync_row_is_tombstone
                                        ,last_change_datetime
                                        )
                                    values( {idColumnsSelects.ToString()}
                                        ,null
                                        ,{this.timestampValue}
                                        ,0::bit
                                        ,now()
                                        )
                                    on conflict({idColumns.ToString()}) do update
                                   SET
                                   ""timestamp"" = {this.timestampValue}
                                    ,sync_row_is_tombstone = 0::bit
                                    ,update_scope_id = null
                                    ,last_change_datetime = now();
                                return NEW;
                                end;
                                $new$;";

            return stringBuilder;
        }
        private string CreateDeleteTriggerAsync()
        {
            var tablename = tableName.Unquoted().ToString();
            var idColumnsSelects = new StringBuilder();
            var idColumns = new StringBuilder();
            //var stringPkAreNull = new StringBuilder();
            string argComma = " ";
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Unquoted().ToString();
                idColumns.AppendLine($"{argComma}{columnName}");
                idColumnsSelects.AppendLine($"{argComma}OLD.{columnName}");
                //stringPkAreNull.Append($"{argAnd}side.{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }
            string stringBuilder = @$"CREATE OR REPLACE FUNCTION {tableName.Schema().Unquoted().ToString()}_delete_trigger_function()
                                    RETURNS trigger
                                    LANGUAGE 'plpgsql'
                                    COST 100
                                    VOLATILE NOT LEAKPROOF
                                AS $new$
                                BEGIN
                                    insert into {trackingName.Schema().Unquoted().ToString()} 
                                    ({idColumns.ToString()}
                                        , update_scope_id
                                        ,""timestamp""
                                        ,sync_row_is_tombstone
                                        ,last_change_datetime
                                        )
                                    values( {idColumnsSelects.ToString()}
                                        ,null
                                        ,{this.timestampValue}
                                        ,0::bit
                                        ,now()
                                        )
                                    on conflict({idColumns.ToString()}) do update
                                   SET
                                   ""timestamp"" = {this.timestampValue}
                                    ,sync_row_is_tombstone = 0::bit
                                    ,update_scope_id = null
                                    ,last_change_datetime = now();
                                return OLD;
                                end;
                                $new$;";

            return stringBuilder;
        }
    }
}