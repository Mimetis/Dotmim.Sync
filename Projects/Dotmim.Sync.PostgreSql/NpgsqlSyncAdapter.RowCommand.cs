using Dotmim.Sync.DatabaseStringParsers;
using Dotmim.Sync.PostgreSql.Builders;
using Npgsql;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.PostgreSql
{
    /// <summary>
    /// Npgsql sync adapter.
    /// </summary>
    public partial class NpgsqlSyncAdapter : DbSyncAdapter
    {
        // ---------------------------------------------------
        // Select Row Command
        // ---------------------------------------------------
        private (DbCommand Command, bool IsBatchCommand) GetSelectRowCommand()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("SELECT ");
            var stringBuilderWhere = new StringBuilder();
            string empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                var columnName = columnParser.QuotedShortName;
                var parameterName = columnParser.NormalizedShortName;

                stringBuilderWhere.Append($@"{empty}side.{columnName} = @{parameterName}");
                empty = " AND ";
            }

            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                var isPrimaryKey = this.TableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.AppendLine($"\tside.{columnParser.QuotedShortName}, ");
                else
                    stringBuilder.AppendLine($"\tbase.{columnParser.QuotedShortName}, ");
            }

            stringBuilder.AppendLine($"\tside.\"sync_row_is_tombstone\" as sync_row_is_tombstone, ");
            stringBuilder.AppendLine($"\tside.\"update_scope_id\" as sync_update_scope_id");
            stringBuilder.AppendLine($"FROM {this.NpgsqlObjectNames.TableQuotedFullName} base");
            stringBuilder.AppendLine($"RIGHT JOIN {this.NpgsqlObjectNames.TrackingTableQuotedFullName} side ON");

            string str2 = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                stringBuilder.Append($"{str2}base.{columnParser.QuotedShortName} = side.{columnParser.QuotedShortName} ");
                str2 = " AND ";
            }

            // stringBuilder.AppendLine();
            stringBuilder.Append(string.Concat("WHERE ", stringBuilderWhere.ToString(), ";"));

            var sqlCommand = new NpgsqlCommand
            {
                CommandType = CommandType.Text,
                CommandText = stringBuilder.ToString(),
            };

            return (sqlCommand, false);
        }

        // ---------------------------------------------------
        // Pre Update Command
        // ---------------------------------------------------

        /// <summary>
        /// Get the NpgsqlCommand that will creates the pg_temp.table_update command.
        /// </summary>
        private (DbCommand Command, bool IsBatchCommand) CreatePreUpdateCommand()
        {
            var storedProcedureName = $"{this.ScopeInfo.Setup?.StoredProceduresPrefix}{this.NpgsqlObjectNames.TableNormalizedFullName}{this.ScopeInfo.Setup?.StoredProceduresSuffix}_";
            var scopeNameWithoutDefaultScope = this.ScopeInfo.Name == SyncOptions.DefaultScopeName ? string.Empty : $"{this.ScopeInfo.Name}_";
            var procName = string.Format(NpgsqlObjectNames.UpdateProcName, this.NpgsqlObjectNames.TableSchemaName, storedProcedureName, scopeNameWithoutDefaultScope);
            var procNameParser = new TableParser(procName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);

            var hasMutableColumns = this.TableDescription.GetMutableColumns(false).Any();

            var sqlCommand = new NpgsqlCommand();
            var stringBuilder = new StringBuilder();

            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();
            var empty = string.Empty;

            stringBuilder.AppendLine($"CREATE OR REPLACE FUNCTION pg_temp.{procNameParser.NormalizedFullName} (");

            foreach (var column in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnParser = new ObjectParser(column.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                var columnType = this.NpgsqlDbMetadata.GetCompatibleColumnTypeDeclarationString(column, this.TableDescription.OriginalProvider);
                stringBuilder.AppendLine($"\t\"in_{columnParser.NormalizedShortName}\" {columnType} = NULL,");
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
            {

                var columnParser = new ObjectParser(c.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                stringBuilder.Append($"p.{columnParser.QuotedShortName}, ");
            }

            stringBuilder.AppendLine($"");
            stringBuilder.AppendLine($"\tside.\"update_scope_id\" as \"sync_update_scope_id\", side.\"timestamp\" as \"sync_timestamp\", side.\"sync_row_is_tombstone\" as \"sync_row_is_tombstone\"");
            stringBuilder.AppendLine($"\tFROM (SELECT ");
            stringBuilder.Append($"\t\t ");
            string comma = string.Empty;
            foreach (var c in this.TableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnParser = new ObjectParser(c.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);

                stringBuilder.Append($"{comma}\"in_{columnParser.NormalizedShortName}\" as {columnParser.QuotedShortName}");
                comma = ", ";
            }

            stringBuilder.AppendLine($") AS p");
            stringBuilder.AppendLine($"\tLEFT JOIN {this.NpgsqlObjectNames.TrackingTableQuotedFullName} side ON ");
            stringBuilder.AppendLine($"\t{NpgsqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "p", "side")}");
            stringBuilder.AppendLine($"\t)");
            stringBuilder.AppendLine($"MERGE INTO {this.NpgsqlObjectNames.TableQuotedFullName} AS base");
            stringBuilder.AppendLine($"USING changes on {NpgsqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "changes", "base")}");
            if (hasMutableColumns)
            {
                stringBuilder.AppendLine($"WHEN MATCHED AND (changes.\"sync_timestamp\" <= sync_min_timestamp OR changes.\"sync_timestamp\" IS NULL OR changes.\"sync_update_scope_id\" =sync_scope_id OR sync_force_write = 1) THEN");
                stringBuilder.AppendLine($"\tUPDATE SET");

                string strSeparator = string.Empty;
                foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false))
                {
                    var columnParser = new ObjectParser(mutableColumn.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);

                    stringBuilder.AppendLine($"\t{strSeparator}{columnParser.QuotedShortName} = changes.{columnParser.QuotedShortName}");
                    strSeparator = ", ";
                }
            }

            stringBuilder.AppendLine($"WHEN NOT MATCHED AND (changes.\"sync_timestamp\" <= sync_min_timestamp OR changes.\"sync_timestamp\" IS NULL OR sync_force_write = 1) THEN");
            stringBuilder.AppendLine($"");
            foreach (var mutableColumn in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                stringBuilderArguments.Append(string.Concat(empty, columnParser.QuotedShortName));
                stringBuilderParameters.Append(string.Concat(empty, $"changes.{columnParser.QuotedShortName}"));
                empty = ", ";
            }

            stringBuilder.AppendLine($"\tINSERT");
            stringBuilder.AppendLine($"\t({stringBuilderArguments})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters});");
            stringBuilder.AppendLine($"");
            stringBuilder.AppendLine($"GET DIAGNOSTICS \"sync_row_count\" = ROW_COUNT;");
            stringBuilder.AppendLine($"");
            stringBuilder.AppendLine($"IF (sync_row_count > 0) THEN");

            string selectPkeys = string.Empty;
            string insertPkeys = string.Empty;
            string insertePkeysValues = string.Empty;
            foreach (var pkeyColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkeyColumn.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                insertPkeys += $"{columnParser.QuotedShortName}, ";
                insertePkeysValues += $"\"changes\".{columnParser.QuotedShortName}, ";
                selectPkeys += $", \"in_{columnParser.NormalizedShortName}\" as {columnParser.QuotedShortName}";
            }

            stringBuilder.AppendLine($"\tMERGE INTO {this.NpgsqlObjectNames.TrackingTableQuotedFullName} AS base");
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
        /// Get the NpgsqlCommand that will creates the pg_temp.table_delete command.
        /// </summary>
        private (DbCommand Command, bool IsBatchCommand) CreatePreDeleteCommand()
        {
            var storedProcedureName = $"{this.ScopeInfo.Setup?.StoredProceduresPrefix}{this.NpgsqlObjectNames.TableNormalizedFullName}{this.ScopeInfo.Setup?.StoredProceduresSuffix}_";
            var scopeNameWithoutDefaultScope = this.ScopeInfo.Name == SyncOptions.DefaultScopeName ? string.Empty : $"{this.ScopeInfo.Name}_";
            var procName = string.Format(NpgsqlObjectNames.DeleteProcName, this.NpgsqlObjectNames.TableSchemaName, storedProcedureName, scopeNameWithoutDefaultScope);
            var procParser = new TableParser(procName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);

            var fullTableName = this.NpgsqlObjectNames.TableQuotedFullName;
            var sqlCommand = new NpgsqlCommand();
            var stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"CREATE OR REPLACE FUNCTION pg_temp.{procParser.NormalizedFullName}(");

            foreach (var column in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(column.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                var columnType = this.NpgsqlDbMetadata.GetCompatibleColumnTypeDeclarationString(column, this.TableDescription.OriginalProvider);
                stringBuilder.AppendLine($"\t\"in_{columnParser.NormalizedShortName}\" {columnType} = NULL,");
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
            stringBuilder.AppendLine($"LEFT JOIN {this.NpgsqlObjectNames.TrackingTableQuotedFullName} side ON {NpgsqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "base", "side")} ");
            stringBuilder.AppendLine($"WHERE {NpgsqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "base", fullTableName)}  ");
            stringBuilder.AppendLine($"AND (side.timestamp <= sync_min_timestamp OR side.timestamp IS NULL OR side.update_scope_id = sync_scope_id OR sync_force_write = 1)");
            stringBuilder.AppendLine($"AND ({NpgsqlManagementUtils.ColumnsAndParameters(this.TableDescription.PrimaryKeys, "base", NpgsqlSyncProvider.NPGSQLPREFIXPARAMETER)});");
            stringBuilder.AppendLine($"");
            stringBuilder.AppendLine($"GET DIAGNOSTICS \"sync_row_count\" = ROW_COUNT;");
            stringBuilder.AppendLine($"IF (sync_row_count > 0) THEN");

            string selectPkeys = string.Empty;
            string insertPkeys = string.Empty;
            string insertePkeysValues = string.Empty;
            foreach (var pkeyColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkeyColumn.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                insertPkeys += $"{columnParser.QuotedShortName}, ";
                insertePkeysValues += $"\"changes\".{columnParser.QuotedShortName}, ";
                selectPkeys += $", \"in_{columnParser.NormalizedShortName}\" as {columnParser.QuotedShortName}";
            }

            stringBuilder.AppendLine($"\tMERGE INTO {this.NpgsqlObjectNames.TrackingTableQuotedFullName} AS base");
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
        private (DbCommand Command, bool IsBatchCommand) GetUpdateRowCommand()
        {
            var storedProcedureName = $"{this.ScopeInfo.Setup?.StoredProceduresPrefix}{this.NpgsqlObjectNames.TableNormalizedFullName}{this.ScopeInfo.Setup?.StoredProceduresSuffix}_";
            var scopeNameWithoutDefaultScope = this.ScopeInfo.Name == SyncOptions.DefaultScopeName ? string.Empty : $"{this.ScopeInfo.Name}_";
            var procName = string.Format(NpgsqlObjectNames.UpdateProcName, this.NpgsqlObjectNames.TableSchemaName, storedProcedureName, scopeNameWithoutDefaultScope);
            var procNameParser = new TableParser(procName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);

            var strCommandText = new StringBuilder();
            strCommandText.Append($"SELECT * FROM pg_temp.{procNameParser.NormalizedFullName}(");

            foreach (var column in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnParser = new ObjectParser(column.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);

                strCommandText.Append($"@{columnParser.NormalizedShortName}, ");
            }

            strCommandText.Append("@sync_scope_id, @sync_force_write, @sync_min_timestamp)");

            var command = new NpgsqlCommand
            {
                CommandType = CommandType.Text,
                CommandText = strCommandText.ToString(),
            };
            return (command, false);
        }

        // ---------------------------------------------------
        // Delete Command
        // ---------------------------------------------------
        private (DbCommand Command, bool IsBatchCommand) GetDeleteRowCommand()
        {
            var storedProcedureName = $"{this.ScopeInfo.Setup?.StoredProceduresPrefix}{this.NpgsqlObjectNames.TableNormalizedFullName}{this.ScopeInfo.Setup?.StoredProceduresSuffix}_";
            var scopeNameWithoutDefaultScope = this.ScopeInfo.Name == SyncOptions.DefaultScopeName ? string.Empty : $"{this.ScopeInfo.Name}_";
            var procName = string.Format(NpgsqlObjectNames.DeleteProcName, this.NpgsqlObjectNames.TableSchemaName, storedProcedureName, scopeNameWithoutDefaultScope);
            var procNameParser = new TableParser(procName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);

            var strCommandText = new StringBuilder();
            strCommandText.Append($"SELECT * FROM pg_temp.{procNameParser.NormalizedFullName}(");

            foreach (var column in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(column.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                strCommandText.Append($"@{columnParser.NormalizedShortName}, ");
            }

            strCommandText.Append("@sync_scope_id, @sync_force_write, @sync_min_timestamp)");

            var command = new NpgsqlCommand
            {
                CommandType = CommandType.Text,
                CommandText = strCommandText.ToString(),
            };
            return (command, false);
        }

        //----------------------------------------------------
    }
}