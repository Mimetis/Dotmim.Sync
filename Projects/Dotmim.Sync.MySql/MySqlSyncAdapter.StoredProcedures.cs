using Dotmim.Sync.Builders;
using Dotmim.Sync.MySql.Builders;
#if NET6_0 || NET8_0
using MySqlConnector;
#elif NETSTANDARD
using MySql.Data.MySqlClient;
#endif
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.MySql
{
    public partial class MySqlSyncAdapter
    {

        /// <summary>
        /// Gets the MySql prefix parameter.
        /// </summary>
        public const string MYSQLPREFIXPARAMETER = "in_";

        // public string GetStoredProcedureCommandName(DbStoredProcedureType storedProcedureType, SyncFilter filter = null)
        // {

        // return storedProcedureType switch
        //    {
        //        DbStoredProcedureType.SelectChanges => string.Format(MySqlObjectNames.SelectChangesProcName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
        //        DbStoredProcedureType.SelectChangesWithFilters => string.Format(MySqlObjectNames.SelectChangesProcNameWithFilters, storedProcedureNormalizedName, scopeNameWithoutDefaultScope, filter.GetFilterName()),
        //        DbStoredProcedureType.SelectInitializedChanges => string.Format(MySqlObjectNames.InitializeChangesProcName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
        //        DbStoredProcedureType.SelectInitializedChangesWithFilters => string.Format(MySqlObjectNames.InitializeChangesProcNameWithFilters, storedProcedureNormalizedName, scopeNameWithoutDefaultScope, filter.GetFilterName()),
        //        DbStoredProcedureType.SelectRow => string.Format(MySqlObjectNames.SelectRowProcName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
        //        DbStoredProcedureType.UpdateRow => string.Format(MySqlObjectNames.UpdateProcName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
        //        DbStoredProcedureType.DeleteRow => string.Format(MySqlObjectNames.DeleteProcName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
        //        _ => null,
        //    };
        // }

        //------------------------------------------------------------------
        // Update command
        //------------------------------------------------------------------

        /// <summary>
        /// Returns the DbCommand to update a row in the table.
        /// </summary>
        public DbCommand CreateUpdateCommand(DbConnection connection, DbTransaction transaction)
        {

            var scopeNameWithoutDefaultScope = this.ScopeInfo.Name == SyncOptions.DefaultScopeName ? string.Empty : $"{this.ScopeInfo.Name}_";
            var storedProcedureNormalizedName = $"{this.ScopeInfo.Setup?.StoredProceduresPrefix}{this.MySqlObjectNames.TableNormalizedFullName}{this.ScopeInfo.Setup?.StoredProceduresSuffix}_";
            var storedProcedureName = string.Format(MySqlObjectNames.UpdateProcName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope);

            // Check if we have mutables columns
            var hasMutableColumns = this.TableDescription.GetMutableColumns(false).Any();

            var sqlCommand = new MySqlCommand();

            var stringBuilder = new StringBuilder();
            this.AddColumnParametersToCommand(sqlCommand);

            var sqlParameter = new MySqlParameter();
            sqlParameter.ParameterName = "sync_scope_id";
            sqlParameter.MySqlDbType = MySqlDbType.Guid;
            sqlParameter.Size = 36;
            sqlCommand.Parameters.Add(sqlParameter);

            sqlParameter = new MySqlParameter();
            sqlParameter.ParameterName = "sync_force_write";
            sqlParameter.MySqlDbType = MySqlDbType.Int32;
            sqlCommand.Parameters.Add(sqlParameter);

            sqlParameter = new MySqlParameter();
            sqlParameter.ParameterName = "sync_min_timestamp";
            sqlParameter.MySqlDbType = MySqlDbType.Int64;
            sqlCommand.Parameters.Add(sqlParameter);

            sqlParameter = new MySqlParameter();
            sqlParameter.ParameterName = "sync_row_count";
            sqlParameter.MySqlDbType = MySqlDbType.Int32;
            sqlParameter.Direction = ParameterDirection.Output;
            sqlCommand.Parameters.Add(sqlParameter);

            var listQuotedPrimaryKeys = new StringBuilder();
            var listColumnsTmp2 = new StringBuilder();
            var listColumnsTmp3 = new StringBuilder();

            var and = string.Empty;
            foreach (var column in this.TableDescription.GetPrimaryKeysColumns())
            {
                var param = this.GetMySqlParameter(column);
                param.ParameterName = $"t_{param.ParameterName}";
                var declar = this.CreateParameterDeclaration(param);
                var columnNameQuoted = ParserName.Parse(column, "`").Quoted().ToString();

                var parameterNameQuoted = ParserName.Parse(param.ParameterName, "`").Quoted().ToString();

                // Primary keys column name, with quote
                listQuotedPrimaryKeys.Append($"{columnNameQuoted}, ");

                // param name without type
                listColumnsTmp2.Append($"{parameterNameQuoted}, ");

                // param name with type
                stringBuilder.AppendLine($"DECLARE {declar};");

                // Param equal IS NULL
                listColumnsTmp3.Append($"{and}{parameterNameQuoted} IS NULL");

                and = " AND ";
            }

            stringBuilder.Append("CREATE PROCEDURE ");
            stringBuilder.Append(storedProcedureName);
            stringBuilder.Append(" (");
            stringBuilder.AppendLine();
            string str = "\n\t";

            foreach (MySqlParameter parameter in sqlCommand.Parameters)
            {
                stringBuilder.Append(string.Concat(str, this.CreateParameterDeclaration(parameter)));
                str = ",\n\t";
            }

            stringBuilder.Append("\n)\nBEGIN\n");

            stringBuilder.AppendLine("DECLARE ts BIGINT;");
            stringBuilder.AppendLine("DECLARE t_update_scope_id VARCHAR(36);");
            stringBuilder.AppendLine($"SELECT {listQuotedPrimaryKeys}");
            stringBuilder.AppendLine($"`timestamp`, `update_scope_id` FROM {this.MySqlObjectNames.TrackingTableQuotedShortName} ");
            stringBuilder.AppendLine($"WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.GetPrimaryKeysColumns(), this.MySqlObjectNames.TrackingTableQuotedShortName)} LIMIT 1 ");
            stringBuilder.AppendLine($"INTO {listColumnsTmp2} ts, t_update_scope_id;");
            stringBuilder.AppendLine();

            if (hasMutableColumns)
            {
                stringBuilder.AppendLine($"UPDATE {this.MySqlObjectNames.TableQuotedShortName}");
                stringBuilder.Append($"SET {MySqlManagementUtils.CommaSeparatedUpdateFromParameters(this.TableDescription)}");
                stringBuilder.Append($"WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.GetPrimaryKeysColumns(), string.Empty)}");
                stringBuilder.AppendLine($" AND (ts <= sync_min_timestamp OR ts IS NULL OR t_update_scope_id  = sync_scope_id OR sync_force_write = 1);");
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SELECT ROW_COUNT() INTO sync_row_count;"); // [AB] LIMIT 1 removed to be compatible with MariaDB 10.3.x
                stringBuilder.AppendLine($"IF (sync_row_count = 0) THEN");
            }

            string empty = string.Empty;
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();
            foreach (var mutableColumn in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn, "`").Quoted().ToString();
                var parameterName = ParserName.Parse(mutableColumn, "`").Quoted().ToString();

                var paramQuotedColumn = ParserName.Parse($"{MYSQLPREFIXPARAMETER}{mutableColumn.ColumnName}", "`");

                stringBuilderArguments.Append(string.Concat(empty, columnName));
                stringBuilderParameters.Append(string.Concat(empty, paramQuotedColumn.Quoted().Normalized().ToString()));
                empty = ", ";
            }

            // If we don't have any mutable column, we can't update, and the Insert
            // will fail if we don't ignore the insert (on Reinitialize for example)
            var ignoreKeyWord = hasMutableColumns ? string.Empty : "IGNORE";
            stringBuilder.AppendLine($"\tINSERT {ignoreKeyWord} INTO {this.MySqlObjectNames.TableQuotedShortName}");
            stringBuilder.AppendLine($"\t({stringBuilderArguments})");
            stringBuilder.AppendLine($"\tSELECT * FROM ( SELECT {stringBuilderParameters}) as TMP ");
            stringBuilder.AppendLine($"\tWHERE ( {listColumnsTmp3} )");
            stringBuilder.AppendLine($"\tOR (ts <= sync_min_timestamp OR ts IS NULL OR t_update_scope_id = sync_scope_id OR sync_force_write = 1)");
            stringBuilder.AppendLine($"\tLIMIT 1;");
            stringBuilder.AppendLine($"");
            stringBuilder.AppendLine($"SELECT ROW_COUNT() INTO sync_row_count;"); // [AB] LIMIT 1 removed to be compatible with MariaDB 10.3.x
            stringBuilder.AppendLine($"");

            if (hasMutableColumns)
                stringBuilder.AppendLine("END IF;");

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"IF (sync_row_count > 0) THEN");
            stringBuilder.AppendLine($"\tUPDATE {this.MySqlObjectNames.TrackingTableQuotedShortName}");
            stringBuilder.AppendLine($"\tSET `update_scope_id` = sync_scope_id, ");
            stringBuilder.AppendLine($"\t\t `sync_row_is_tombstone` = 0, ");
            stringBuilder.AppendLine($"\t\t `timestamp` = {MySqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine($"\t\t `last_change_datetime` = now() ");
            stringBuilder.AppendLine($"\tWHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.GetPrimaryKeysColumns(), string.Empty)};");
            stringBuilder.AppendLine($"END IF;");
            stringBuilder.Append("\nEND");

            sqlCommand.CommandText = stringBuilder.ToString();

            return this.CreateProcedureCommand(this.BuildUpdateCommand, storedProcedureName, hasMutableColumns, connection, transaction);
        }

        /// <summary>
        /// From a SqlParameter, create the declaration.
        /// </summary>
        internal string CreateParameterDeclaration(MySqlParameter param)
        {

            var tmpColumn = new SyncColumn(param.ParameterName)
            {
                OriginalDbType = param.MySqlDbType.ToString(),
                OriginalTypeName = param.MySqlDbType.ToString().ToLowerInvariant(),
                MaxLength = param.Size,
                Precision = param.Precision,
                Scale = param.Scale,
                DbType = (int)param.DbType,
                ExtraProperty1 = string.IsNullOrEmpty(param.SourceColumn) ? null : param.SourceColumn,
            };

            var stringBuilder3 = new StringBuilder();
            string columnDeclarationString = this.MySqlDbMetadata.GetCompatibleColumnTypeDeclarationString(tmpColumn, this.TableDescription.OriginalProvider);

            string output = string.Empty;
            string isNull = string.Empty;
            string defaultValue = string.Empty;

            if (param.Direction == ParameterDirection.Output || param.Direction == ParameterDirection.InputOutput)
                output = "OUT ";

            var parameterName = ParserName.Parse(param.ParameterName, "`").Quoted().ToString();

            stringBuilder3.Append($"{output}{parameterName} {columnDeclarationString} {isNull} {defaultValue}");

            return stringBuilder3.ToString();
        }

        private void AddColumnParametersToCommand(MySqlCommand sqlCommand)
        {
            foreach (var column in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
                sqlCommand.Parameters.Add(this.GetMySqlParameter(column));
        }
    }
}