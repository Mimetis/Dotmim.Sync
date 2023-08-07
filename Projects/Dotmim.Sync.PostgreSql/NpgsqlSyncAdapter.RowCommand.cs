using Npgsql;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using System.Text;
using Dotmim.Sync.Builders;
using NpgsqlTypes;
using System.Linq;
using static Npgsql.Replication.PgOutput.Messages.RelationMessage;

namespace Dotmim.Sync.PostgreSql
{
    public partial class NpgsqlSyncAdapter : DbSyncAdapter
    {
        // ---------------------------------------------------
        // Select Row Command
        // ---------------------------------------------------

        private (DbCommand, bool) GetSelectRowCommand()
        {
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(TableName);

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("SELECT ");
            var stringBuilderWhere = new StringBuilder();
            string empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn, "\"").Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn, "\"").Unquoted().Normalized().ToString();

                stringBuilderWhere.Append($@"{empty}side.{columnName} = @{parameterName}");
                empty = " AND ";
            }
            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                var isPrimaryKey = this.TableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.AppendLine($"\tside.{columnName}, ");
                else
                    stringBuilder.AppendLine($"\tbase.{columnName}, ");
            }
            stringBuilder.AppendLine($"\tside.\"sync_row_is_tombstone\" as sync_row_is_tombstone, ");
            stringBuilder.AppendLine($"\tside.\"update_scope_id\" as sync_update_scope_id");
            stringBuilder.AppendLine($"FROM \"{schema}\".{TableName.Quoted()} base");
            stringBuilder.AppendLine($"RIGHT JOIN \"{schema}\".{TrackingTableName.Quoted()} side ON");

            string str2 = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn, "\"").Quoted().ToString();
                stringBuilder.Append($"{str2}base.{columnName} = side.{columnName} ");
                str2 = " AND ";
            }
            //stringBuilder.AppendLine();
            stringBuilder.Append(string.Concat("WHERE ", stringBuilderWhere.ToString(), ";"));

            var sqlCommand = new NpgsqlCommand
            {
                CommandType = CommandType.Text,
                CommandText = stringBuilder.ToString()
            };

            return (sqlCommand, false);
        }


        // ---------------------------------------------------
        // Pre Update Command
        // ---------------------------------------------------

        /// <summary>
        /// Get the NpgsqlCommand that will creates the pg_temp.table_update command
        /// </summary>
        /// <returns></returns>
        private (DbCommand, bool) CreatePreUpdateCommand()
        {
            var storedProcedureName = $"{this.Setup?.StoredProceduresPrefix}{TableName.Unquoted().Normalized()}{this.Setup?.StoredProceduresSuffix}_";
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(TableName);
            var scopeNameWithoutDefaultScope = this.ScopeName == SyncOptions.DefaultScopeName ? "" : $"{this.ScopeName}_";
            var procName = string.Format(updateProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope);
            var procNameQuoted = ParserName.Parse(procName, "\"").Quoted().ToString();

            var hasMutableColumns = this.TableDescription.GetMutableColumns(false).Any();

            var trackingTableQuoted = ParserName.Parse(this.TrackingTableName.ToString(), "\"").Quoted().ToString();
            var tableQuoted = ParserName.Parse(TableName.ToString(), "\"").Quoted().ToString();

            var sqlCommand = new NpgsqlCommand();
            var stringBuilder = new StringBuilder();

            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();
            var empty = string.Empty;

            stringBuilder.AppendLine($"CREATE OR REPLACE FUNCTION pg_temp.{procNameQuoted} (");

            foreach (var column in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(column, "\"").Unquoted().Normalized().ToString();
                var columnType = this.NpgsqlDbMetadata.GetCompatibleColumnTypeDeclarationString(column, this.TableDescription.OriginalProvider);
                stringBuilder.AppendLine($"\t\"in_{columnName}\" {columnType} = NULL,");
            }

            stringBuilder.AppendLine($"\tsync_scope_id Uuid = NULL,");
            stringBuilder.AppendLine($"\tsync_force_write Bigint = NULL,");
            stringBuilder.AppendLine($"\tsync_min_timestamp Bigint = NULL,");
            stringBuilder.AppendLine($"\tout sync_row_count Integer,");
            stringBuilder.AppendLine($"\tout sync_error_text Text)");
            stringBuilder.AppendLine($"AS $BODY$");
            stringBuilder.AppendLine($"DECLARE sync_error_sqlcontext text;");
            stringBuilder.AppendLine($"BEGIN");
            stringBuilder.AppendLine("WITH changes AS (");
            stringBuilder.Append("\tSELECT ");
            foreach (var c in this.TableDescription.Columns.Where(col => !col.IsReadOnly))
                stringBuilder.Append($"p.{ParserName.Parse(c, "\"").Quoted()}, ");

            stringBuilder.AppendLine($"");
            stringBuilder.AppendLine($"\tside.\"update_scope_id\" as \"sync_update_scope_id\", side.\"timestamp\" as \"sync_timestamp\", side.\"sync_row_is_tombstone\" as \"sync_row_is_tombstone\"");
            stringBuilder.AppendLine($"\tFROM (SELECT ");
            stringBuilder.Append($"\t\t ");
            string comma = "";
            foreach (var c in this.TableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                stringBuilder.Append($"{comma}\"in_{ParserName.Parse(c).Unquoted().Normalized()}\" as {ParserName.Parse(c, "\"").Quoted()}");
                comma = ", ";
            }
            stringBuilder.AppendLine($") AS p");
            stringBuilder.AppendLine($"\tLEFT JOIN \"{schema}\".{this.TrackingTableName.Quoted()} side ON ");
            stringBuilder.AppendLine($"\t{NpgsqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "p", "side")}");
            stringBuilder.AppendLine($"\t)");
            stringBuilder.AppendLine($"MERGE INTO \"{schema}\".{TableName.Quoted()} AS base");
            stringBuilder.AppendLine($"USING changes on {NpgsqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "changes", "base")}");
            if (hasMutableColumns)
            {
                stringBuilder.AppendLine($"WHEN MATCHED AND (changes.\"sync_timestamp\" <= sync_min_timestamp OR changes.\"sync_timestamp\" IS NULL OR changes.\"sync_update_scope_id\" =sync_scope_id OR sync_force_write = 1) THEN");
                stringBuilder.AppendLine($"\tUPDATE SET");

                string strSeparator = "";
                foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false))
                {
                    stringBuilder.AppendLine($"\t{strSeparator}{ParserName.Parse(mutableColumn, "\"").Quoted()} = changes.{ParserName.Parse(mutableColumn, "\"").Quoted()}");
                    strSeparator = ", ";
                }
            }

            stringBuilder.AppendLine($"WHEN NOT MATCHED AND (changes.\"sync_timestamp\" <= sync_min_timestamp OR changes.\"sync_timestamp\" IS NULL OR sync_force_write = 1) THEN");
            stringBuilder.AppendLine($"");
            foreach (var mutableColumn in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                stringBuilderArguments.Append(string.Concat(empty, ParserName.Parse(mutableColumn, "\"").Quoted()));
                stringBuilderParameters.Append(string.Concat(empty, $"changes.{ParserName.Parse(mutableColumn, "\"").Quoted()}"));
                empty = ", ";
            }
            stringBuilder.AppendLine($"\tINSERT");
            stringBuilder.AppendLine($"\t({stringBuilderArguments})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters});");
            stringBuilder.AppendLine($"");
            stringBuilder.AppendLine($"GET DIAGNOSTICS \"sync_row_count\" = ROW_COUNT;");
            stringBuilder.AppendLine($"");
            stringBuilder.AppendLine($"IF (sync_row_count > 0) THEN");

            string selectPkeys = "";
            string insertPkeys = "";
            string insertePkeysValues = "";
            foreach(var pkeyColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkeyColumn, "\"").Unquoted().Normalized();
                insertPkeys += $"{ParserName.Parse(pkeyColumn, "\"").Quoted()}, ";
                insertePkeysValues += $"\"changes\".{ParserName.Parse(pkeyColumn, "\"").Quoted()}, ";
                selectPkeys += $", \"in_{ParserName.Parse(pkeyColumn, "\"").Unquoted().Normalized()}\" as {ParserName.Parse(pkeyColumn, "\"").Quoted()}";
            }

            stringBuilder.AppendLine($"\tMERGE INTO \"{schema}\".{trackingTableQuoted} AS base");
            stringBuilder.AppendLine($"\tUSING (SELECT sync_scope_id {selectPkeys}) AS changes on {NpgsqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "changes", "base")}");
            stringBuilder.AppendLine($"\t\tWHEN MATCHED THEN");
            stringBuilder.AppendLine($"\t\tUPDATE ");
            stringBuilder.AppendLine($"\t\tSET \"update_scope_id\" = \"changes\".\"sync_scope_id\", ");
            stringBuilder.AppendLine($"\t\t \"sync_row_is_tombstone\" = 0, ");
            stringBuilder.AppendLine($"\t\t \"timestamp\" = {TimestampValue}, ");
            stringBuilder.AppendLine($"\t\t \"last_change_datetime\" = now() ");
            stringBuilder.AppendLine($"\tWHEN NOT MATCHED THEN");
            stringBuilder.AppendLine($"\t\tINSERT ({insertPkeys}\"update_scope_id\", \"sync_row_is_tombstone\", \"timestamp\", \"last_change_datetime\")");
            stringBuilder.AppendLine($"\t\tVALUES ({insertePkeysValues}\"changes\".\"sync_scope_id\", 0, {TimestampValue}, now());");
            stringBuilder.AppendLine($"END IF;");
            stringBuilder.AppendLine($"");
            stringBuilder.AppendLine($"EXCEPTION WHEN OTHERS THEN");
            stringBuilder.AppendLine($"\tGET STACKED DIAGNOSTICS sync_error_text = MESSAGE_TEXT, sync_error_sqlcontext = PG_EXCEPTION_CONTEXT;");
            stringBuilder.AppendLine($"\t\"sync_error_text\" = \"sync_error_text\" || '. ' || \"sync_error_sqlcontext\";");
            stringBuilder.AppendLine($"\t\"sync_row_count\"=-1;");
            stringBuilder.AppendLine($"END; ");
            stringBuilder.AppendLine($"$BODY$ LANGUAGE 'plpgsql';");

            sqlCommand.CommandType = CommandType.Text;
            sqlCommand.CommandText = stringBuilder.ToString();
            return (sqlCommand, false);
        }


        // ---------------------------------------------------
        // Pre Delete Command
        // ---------------------------------------------------

        /// <summary>
        /// Get the NpgsqlCommand that will creates the pg_temp.table_delete command
        /// </summary>
        /// <returns></returns>
        private (DbCommand, bool) CreatePreDeleteCommand()
        {
            var storedProcedureName = $"{this.Setup?.StoredProceduresPrefix}{TableName.Unquoted().Normalized()}{this.Setup?.StoredProceduresSuffix}_";
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(TableName);
            var scopeNameWithoutDefaultScope = this.ScopeName == SyncOptions.DefaultScopeName ? "" : $"{this.ScopeName}_";
            var procName = string.Format(deleteProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope);
            var procNameQuoted = ParserName.Parse(procName, "\"").Quoted().ToString();

            var fullTableName = $"\"{schema}\".{TableName.Quoted()}";
            var sqlCommand = new NpgsqlCommand();
            var stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"CREATE OR REPLACE FUNCTION pg_temp.{procNameQuoted}(");

            foreach (var column in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(column, "\"").Unquoted().Normalized().ToString();
                var columnType = this.NpgsqlDbMetadata.GetCompatibleColumnTypeDeclarationString(column, this.TableDescription.OriginalProvider);
                stringBuilder.AppendLine($"\t\"in_{columnName}\" {columnType} = NULL,");
            }

            stringBuilder.AppendLine($"\tsync_scope_id Uuid = NULL,");
            stringBuilder.AppendLine($"\tsync_force_write Bigint = NULL,");
            stringBuilder.AppendLine($"\tsync_min_timestamp Bigint = NULL,");
            stringBuilder.AppendLine($"\tout sync_row_count Integer,");
            stringBuilder.AppendLine($"\tout sync_error_text Text)");
            stringBuilder.AppendLine($"AS $BODY$ ");
            stringBuilder.AppendLine($"DECLARE sync_error_sqlcontext text;");
            stringBuilder.AppendLine($"BEGIN");
            stringBuilder.AppendLine($"");
            stringBuilder.AppendLine($"DELETE from {fullTableName} ");
            stringBuilder.AppendLine($"USING {fullTableName} base");
            stringBuilder.AppendLine($"LEFT JOIN \"{schema}\".{TrackingTableName.Quoted()} side ON {NpgsqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "base", "side")} ");
            stringBuilder.AppendLine($"WHERE {NpgsqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "base", fullTableName)}  ");
            stringBuilder.AppendLine($"AND (side.timestamp <= sync_min_timestamp OR side.timestamp IS NULL OR side.update_scope_id = sync_scope_id OR sync_force_write = 1)");
            stringBuilder.AppendLine($"AND ({NpgsqlManagementUtils.ColumnsAndParameters(this.TableDescription.PrimaryKeys, "base", NpgsqlSyncProvider.NPGSQL_PREFIX_PARAMETER)});");
            stringBuilder.AppendLine($"");
            stringBuilder.AppendLine($"GET DIAGNOSTICS \"sync_row_count\" = ROW_COUNT;");
            stringBuilder.AppendLine($"IF (sync_row_count > 0) THEN");

            string selectPkeys = "";
            string insertPkeys = "";
            string insertePkeysValues = "";
            foreach (var pkeyColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkeyColumn, "\"").Unquoted().Normalized();
                insertPkeys += $"{ParserName.Parse(pkeyColumn, "\"").Quoted()}, ";
                insertePkeysValues += $"\"changes\".{ParserName.Parse(pkeyColumn, "\"").Quoted()}, ";
                selectPkeys += $", \"in_{ParserName.Parse(pkeyColumn, "\"").Unquoted().Normalized()}\" as {ParserName.Parse(pkeyColumn, "\"").Quoted()}";
            }

            stringBuilder.AppendLine($"\tMERGE INTO \"{schema}\".{TrackingTableName.Quoted()} AS base");
            stringBuilder.AppendLine($"\tUSING (SELECT sync_scope_id {selectPkeys}) AS changes on {NpgsqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "changes", "base")}");
            stringBuilder.AppendLine($"\t\tWHEN MATCHED THEN");
            stringBuilder.AppendLine($"\t\tUPDATE ");
            stringBuilder.AppendLine($"\t\tSET \"update_scope_id\" = \"changes\".\"sync_scope_id\", ");
            stringBuilder.AppendLine($"\t\t \"sync_row_is_tombstone\" = 1, ");
            stringBuilder.AppendLine($"\t\t \"timestamp\" = {TimestampValue}, ");
            stringBuilder.AppendLine($"\t\t \"last_change_datetime\" = now() ");
            stringBuilder.AppendLine($"\tWHEN NOT MATCHED THEN");
            stringBuilder.AppendLine($"\t\tINSERT ({insertPkeys}\"update_scope_id\", \"sync_row_is_tombstone\", \"timestamp\", \"last_change_datetime\")");
            stringBuilder.AppendLine($"\t\tVALUES ({insertePkeysValues}\"changes\".\"sync_scope_id\", 1, {TimestampValue}, now());");
            stringBuilder.AppendLine($"END IF;");
            stringBuilder.AppendLine($"");
            stringBuilder.AppendLine($"EXCEPTION WHEN OTHERS THEN");
            stringBuilder.AppendLine($"\tGET STACKED DIAGNOSTICS sync_error_text = MESSAGE_TEXT, sync_error_sqlcontext = PG_EXCEPTION_CONTEXT;");
            stringBuilder.AppendLine($"\t\"sync_error_text\" = \"sync_error_text\" || '. ' || \"sync_error_sqlcontext\";");
            stringBuilder.AppendLine($"\t\"sync_row_count\"=-1;");
            stringBuilder.AppendLine($"END; ");
            stringBuilder.AppendLine($"$BODY$ LANGUAGE 'plpgsql';");
            sqlCommand.CommandType = CommandType.Text;
            sqlCommand.CommandText = stringBuilder.ToString();
            return (sqlCommand, false);
        }

        // ---------------------------------------------------
        // Update Command
        // ---------------------------------------------------

        private (DbCommand, bool) GetUpdateRowCommand()
        {
            var storedProcedureName = $"{this.Setup?.StoredProceduresPrefix}{TableName.Unquoted().Normalized()}{this.Setup?.StoredProceduresSuffix}_";
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(TableName);
            var scopeNameWithoutDefaultScope = this.ScopeName == SyncOptions.DefaultScopeName ? "" : $"{this.ScopeName}_";
            var procName = string.Format(updateProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope);
            var procNameQuoted = ParserName.Parse(procName, "\"").Quoted().ToString();

            var strCommandText = new StringBuilder();
            strCommandText.Append($"SELECT * FROM pg_temp.{procNameQuoted}(");

            foreach (var column in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
                strCommandText.Append($"@{ParserName.Parse(column).Unquoted().Normalized()}, ");

            strCommandText.Append("@sync_scope_id, @sync_force_write, @sync_min_timestamp)");

            var command = new NpgsqlCommand
            {
                CommandType = CommandType.Text,
                CommandText = strCommandText.ToString()
            };
            return (command, false);
        }

        // ---------------------------------------------------
        // Delete Command
        // ---------------------------------------------------

        private (DbCommand, bool) GetDeleteRowCommand()
        {
            var storedProcedureName = $"{this.Setup?.StoredProceduresPrefix}{TableName.Unquoted().Normalized()}{this.Setup?.StoredProceduresSuffix}_";
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(TableName);
            var scopeNameWithoutDefaultScope = this.ScopeName == SyncOptions.DefaultScopeName ? "" : $"{this.ScopeName}_";
            var procName = string.Format(deleteProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope);
            var procNameQuoted = ParserName.Parse(procName, "\"").Quoted().ToString();

            var strCommandText = new StringBuilder();
            strCommandText.Append($"SELECT * FROM pg_temp.{procNameQuoted}(");

            foreach (var column in this.TableDescription.GetPrimaryKeysColumns())
                strCommandText.Append($"@{ParserName.Parse(column).Unquoted().Normalized()}, ");

            strCommandText.Append("@sync_scope_id, @sync_force_write, @sync_min_timestamp)");

            var command = new NpgsqlCommand
            {
                CommandType = CommandType.Text,
                CommandText = strCommandText.ToString()
            };
            return (command, false);
        }

        //----------------------------------------------------

    }
}
