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

                stringBuilderWhere.Append(empty).Append(@"side.").Append(columnName).Append(@" = @").Append(parameterName);
                empty = " AND ";
            }
            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                var isPrimaryKey = this.TableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.Append("\tside.").Append(columnName).AppendLine(", ");
                else
                    stringBuilder.Append("\tbase.").Append(columnName).AppendLine(", ");
            }
            stringBuilder.AppendLine($"\tside.\"sync_row_is_tombstone\" as sync_row_is_tombstone, ");
            stringBuilder.AppendLine($"\tside.\"update_scope_id\" as sync_update_scope_id");
            stringBuilder.Append("FROM \"").Append(schema).Append("\".").Append(TableName.Quoted()).AppendLine(" base");
            stringBuilder.Append("RIGHT JOIN \"").Append(schema).Append("\".").Append(TrackingTableName.Quoted()).AppendLine(" side ON");

            string str2 = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn, "\"").Quoted().ToString();
                stringBuilder.Append(str2).Append("base.").Append(columnName).Append(" = side.").Append(columnName).Append(' ');
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

            stringBuilder.Append("CREATE OR REPLACE FUNCTION pg_temp.").Append(procNameQuoted).AppendLine(" (");

            foreach (var column in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(column, "\"").Unquoted().Normalized().ToString();
                var columnType = this.NpgsqlDbMetadata.GetCompatibleColumnTypeDeclarationString(column, this.TableDescription.OriginalProvider);
                stringBuilder.Append("\t\"in_").Append(columnName).Append("\" ").Append(columnType).AppendLine(" = NULL,");
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
                stringBuilder.Append("p.").Append(ParserName.Parse(c, "\"").Quoted()).Append(", ");

            stringBuilder.AppendLine($"");
            stringBuilder.AppendLine($"\tside.\"update_scope_id\" as \"sync_update_scope_id\", side.\"timestamp\" as \"sync_timestamp\", side.\"sync_row_is_tombstone\" as \"sync_row_is_tombstone\"");
            stringBuilder.AppendLine($"\tFROM (SELECT ");
            stringBuilder.Append($"\t\t ");
            string comma = "";
            foreach (var c in this.TableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                stringBuilder.Append(comma).Append("\"in_").Append(ParserName.Parse(c).Unquoted().Normalized()).Append("\" as ").Append(ParserName.Parse(c, "\"").Quoted());
                comma = ", ";
            }
            stringBuilder.AppendLine($") AS p");
            stringBuilder.Append("\tLEFT JOIN \"").Append(schema).Append("\".").Append(this.TrackingTableName.Quoted()).AppendLine(" side ON ");
            stringBuilder.Append('\t').AppendLine(NpgsqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "p", "side"));
            stringBuilder.AppendLine($"\t)");
            stringBuilder.Append("MERGE INTO \"").Append(schema).Append("\".").Append(TableName.Quoted()).AppendLine(" AS base");
            stringBuilder.Append("USING changes on ").AppendLine(NpgsqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "changes", "base"));
            if (hasMutableColumns)
            {
                stringBuilder.AppendLine($"WHEN MATCHED AND (changes.\"sync_timestamp\" <= sync_min_timestamp OR changes.\"sync_timestamp\" IS NULL OR changes.\"sync_update_scope_id\" =sync_scope_id OR sync_force_write = 1) THEN");
                stringBuilder.AppendLine($"\tUPDATE SET");

                string strSeparator = "";
                foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false))
                {
                    stringBuilder.Append('\t').Append(strSeparator).Append(ParserName.Parse(mutableColumn, "\"").Quoted()).Append(" = changes.").Append(ParserName.Parse(mutableColumn, "\"").Quoted()).AppendLine();
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
            stringBuilder.Append("\t(").Append(stringBuilderArguments).AppendLine(")");
            stringBuilder.Append("\tVALUES (").Append(stringBuilderParameters).AppendLine(");");
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

            stringBuilder.Append("\tMERGE INTO \"").Append(schema).Append("\".").Append(trackingTableQuoted).AppendLine(" AS base");
            stringBuilder.Append("\tUSING (SELECT sync_scope_id ").Append(selectPkeys).Append(") AS changes on ").AppendLine(NpgsqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "changes", "base"));
            stringBuilder.AppendLine($"\t\tWHEN MATCHED THEN");
            stringBuilder.AppendLine($"\t\tUPDATE ");
            stringBuilder.AppendLine($"\t\tSET \"update_scope_id\" = \"changes\".\"sync_scope_id\", ");
            stringBuilder.AppendLine($"\t\t \"sync_row_is_tombstone\" = 0, ");
            stringBuilder.AppendLine($"\t\t \"timestamp\" = {TimestampValue}, ");
            stringBuilder.AppendLine($"\t\t \"last_change_datetime\" = now() ");
            stringBuilder.AppendLine($"\tWHEN NOT MATCHED THEN");
            stringBuilder.Append("\t\tINSERT (").Append(insertPkeys).AppendLine("\"update_scope_id\", \"sync_row_is_tombstone\", \"timestamp\", \"last_change_datetime\")");
            stringBuilder.Append("\t\tVALUES (").Append(insertePkeysValues).Append("\"changes\".\"sync_scope_id\", 0, ").Append(TimestampValue).AppendLine(", now());");
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

            var sqlCommand = new NpgsqlCommand();
            var stringBuilder = new StringBuilder();

            stringBuilder.Append("CREATE OR REPLACE FUNCTION pg_temp.").Append(procNameQuoted).AppendLine("(");

            foreach (var column in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(column, "\"").Unquoted().Normalized().ToString();
                var columnType = this.NpgsqlDbMetadata.GetCompatibleColumnTypeDeclarationString(column, this.TableDescription.OriginalProvider);
                stringBuilder.Append("\t\"in_").Append(columnName).Append("\" ").Append(columnType).AppendLine(" = NULL,");
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
            stringBuilder.Append("DELETE from \"").Append(schema).Append("\".").Append(TableName.Quoted()).AppendLine(" base");
            stringBuilder.Append("USING \"").Append(schema).Append("\".").Append(TrackingTableName.Quoted()).AppendLine(" side ");
            stringBuilder.Append("WHERE ").Append(NpgsqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "base", "side")).AppendLine(" ");
            stringBuilder.AppendLine($"AND (side.timestamp <= sync_min_timestamp OR side.timestamp IS NULL OR side.update_scope_id = sync_scope_id OR sync_force_write = 1)");
            stringBuilder.Append("AND (").Append(NpgsqlManagementUtils.ColumnsAndParameters(this.TableDescription.PrimaryKeys, "base", NpgsqlSyncProvider.NPGSQL_PREFIX_PARAMETER)).AppendLine(");");
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

            stringBuilder.Append("\tMERGE INTO \"").Append(schema).Append("\".").Append(TrackingTableName.Quoted()).AppendLine(" AS base");
            stringBuilder.Append("\tUSING (SELECT sync_scope_id ").Append(selectPkeys).Append(") AS changes on ").AppendLine(NpgsqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "changes", "base"));
            stringBuilder.AppendLine($"\t\tWHEN MATCHED THEN");
            stringBuilder.AppendLine($"\t\tUPDATE ");
            stringBuilder.AppendLine($"\t\tSET \"update_scope_id\" = \"changes\".\"sync_scope_id\", ");
            stringBuilder.AppendLine($"\t\t \"sync_row_is_tombstone\" = 1, ");
            stringBuilder.AppendLine($"\t\t \"timestamp\" = {TimestampValue}, ");
            stringBuilder.AppendLine($"\t\t \"last_change_datetime\" = now() ");
            stringBuilder.AppendLine($"\tWHEN NOT MATCHED THEN");
            stringBuilder.Append("\t\tINSERT (").Append(insertPkeys).AppendLine("\"update_scope_id\", \"sync_row_is_tombstone\", \"timestamp\", \"last_change_datetime\")");
            stringBuilder.Append("\t\tVALUES (").Append(insertePkeysValues).Append("\"changes\".\"sync_scope_id\", 1, ").Append(TimestampValue).AppendLine(", now());");
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
            strCommandText.Append("SELECT * FROM pg_temp.").Append(procNameQuoted).Append('(');

            foreach (var column in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
                strCommandText.Append('@').Append(ParserName.Parse(column).Unquoted().Normalized()).Append(", ");

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
            strCommandText.Append("SELECT * FROM pg_temp.").Append(procNameQuoted).Append('(');

            foreach (var column in this.TableDescription.GetPrimaryKeysColumns())
                strCommandText.Append('@').Append(ParserName.Parse(column).Unquoted().Normalized()).Append(", ");

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
