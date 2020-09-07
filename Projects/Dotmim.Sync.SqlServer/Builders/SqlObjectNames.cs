using Dotmim.Sync.Builders;
using Dotmim.Sync.SqlServer.Manager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlObjectNames
    {
        internal const string insertTriggerName = "[{0}].[{1}_insert_trigger]";
        internal const string updateTriggerName = "[{0}].[{1}_update_trigger]";
        internal const string deleteTriggerName = "[{0}].[{1}_delete_trigger]";

        internal const string selectChangesProcName = "[{0}].[{1}_changes]";
        internal const string selectChangesProcNameWithFilters = "[{0}].[{1}_{2}_changes]";

        internal const string initializeChangesProcName = "[{0}].[{1}_initialize]";
        internal const string initializeChangesProcNameWithFilters = "[{0}].[{1}_{2}_initialize]";

        internal const string selectRowProcName = "[{0}].[{1}_selectrow]";

        internal const string insertProcName = "[{0}].[{1}_insert]";
        internal const string updateProcName = "[{0}].[{1}_update]";
        internal const string deleteProcName = "[{0}].[{1}_delete]";

        internal const string insertMetadataProcName = "[{0}].[{1}_insertmetadata]";
        internal const string updateMetadataProcName = "[{0}].[{1}_updatemetadata]";
        internal const string deleteMetadataProcName = "[{0}].[{1}_deletemetadata]";

        internal const string resetMetadataProcName = "[{0}].[{1}_reset]";

        internal const string bulkTableTypeName = "[{0}].[{1}_BulkType]";
        internal const string bulkInsertProcName = "[{0}].[{1}_bulkinsert]";
        internal const string bulkUpdateProcName = "[{0}].[{1}_bulkupdate]";
        internal const string bulkDeleteProcName = "[{0}].[{1}_bulkdelete]";

        internal const string disableConstraintsText = "ALTER TABLE {0} NOCHECK CONSTRAINT ALL";
        internal const string enableConstraintsText = "ALTER TABLE {0} CHECK CONSTRAINT ALL";
        private readonly ParserName tableName;
        private readonly ParserName trackingName;

        private readonly SqlDbMetadata sqlDbMetadata = new SqlDbMetadata();

        //internal const string disableConstraintsText = "sp_msforeachtable";
        //internal const string enableConstraintsText = "sp_msforeachtable";

        Dictionary<DbCommandType, (string name, bool isStoredProcedure)> names = new Dictionary<DbCommandType, (string name, bool isStoredProcedure)>();
        
        public SyncTable TableDescription { get; }
        public SyncSetup Setup { get; }

        public void AddName(DbCommandType objectType, string name, bool isStoredProcedure)
        {
            if (names.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            names.Add(objectType, (name, isStoredProcedure));
        }
        public (string name, bool isStoredProcedure) GetCommandName(DbCommandType objectType, SyncFilter filter = null)
        {
            if (!names.ContainsKey(objectType))
                throw new Exception("Yous should provide a value for all DbCommandName");

            (var commandName, var isStoredProc) = names[objectType];

            // concat filter name
            if (filter != null)
                commandName = string.Format(commandName, filter.GetFilterName());

            return (commandName, isStoredProc);
        }

        public SqlObjectNames(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
        {
            this.TableDescription = tableDescription;
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.Setup = setup;
            SetDefaultNames();
        }

        /// <summary>
        /// Set the default stored procedures names
        /// </summary>
        private void SetDefaultNames()
        {
            var pref = this.Setup.StoredProceduresPrefix;
            var suf = this.Setup.StoredProceduresSuffix;
            var tpref = this.Setup.TriggersPrefix;
            var tsuf = this.Setup.TriggersSuffix;

            var tableName = ParserName.Parse(TableDescription);

            var schema = string.IsNullOrEmpty(tableName.SchemaName) ? "dbo" : tableName.SchemaName;

            this.AddName(DbCommandType.SelectChanges, string.Format(selectChangesProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);
            this.AddName(DbCommandType.SelectChangesWithFilters, string.Format(selectChangesProcNameWithFilters, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}", "{0}"), true);

            this.AddName(DbCommandType.SelectInitializedChanges, string.Format(initializeChangesProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);
            this.AddName(DbCommandType.SelectInitializedChangesWithFilters, string.Format(initializeChangesProcNameWithFilters, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}", "{0}"), true);

            this.AddName(DbCommandType.SelectRow, string.Format(selectRowProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);
            //this.AddName(DbCommandType.UpdateRow, string.Format(updateProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);
            this.AddName(DbCommandType.UpdateRow, CreateUpdateCommand(), false);

            this.AddName(DbCommandType.DeleteRow, string.Format(deleteProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);
            this.AddName(DbCommandType.DeleteMetadata, string.Format(deleteMetadataProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);
            this.AddName(DbCommandType.Reset, string.Format(resetMetadataProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);

            this.AddName(DbCommandType.InsertTrigger, string.Format(insertTriggerName, schema, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}"), true);
            this.AddName(DbCommandType.UpdateTrigger, string.Format(updateTriggerName, schema, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}"), true);
            this.AddName(DbCommandType.DeleteTrigger, string.Format(deleteTriggerName, schema, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}"), true);

            this.AddName(DbCommandType.BulkTableType, string.Format(bulkTableTypeName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);
            this.AddName(DbCommandType.BulkUpdateRows, string.Format(bulkUpdateProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);
            this.AddName(DbCommandType.BulkDeleteRows, string.Format(bulkDeleteProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);

            this.AddName(DbCommandType.DisableConstraints, string.Format(disableConstraintsText, ParserName.Parse(TableDescription).Schema().Quoted().ToString()), false);
            this.AddName(DbCommandType.EnableConstraints, string.Format(enableConstraintsText, ParserName.Parse(TableDescription).Schema().Quoted().ToString()), false);

            this.AddName(DbCommandType.UpdateUntrackedRows, CreateUpdateUntrackedRowsCommand(), false);
            this.AddName(DbCommandType.UpdateMetadata, CreateUpdateMetadataCommand(), false);
        }


        private string CreateUpdateCommand2()
        {
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();
            var stringBuilderParametersValues = new StringBuilder();
            string empty = string.Empty;

            string str1 = SqlManagementUtils.JoinOneTablesOnParametersValues(this.TableDescription.PrimaryKeys, "[side]");
            string str2 = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[c]", "[base]");
            string str7 = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[p]", "[side]");

            // Generate Update command
            var stringBuilder = new StringBuilder();

            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                var columnParameterName = ParserName.Parse(mutableColumn).Unquoted().Normalized().ToString();
                stringBuilderParametersValues.Append($"{empty}@{columnParameterName} as {columnName}");
                stringBuilderArguments.Append($"{empty}{columnName}");
                stringBuilderParameters.Append($"{empty}[c].{columnName}");
                empty = ", ";
            }

            stringBuilder.AppendLine($"INSERT INTO {tableName.Quoted().ToString()}");
            stringBuilder.AppendLine($"({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"SELECT {stringBuilderParameters.ToString()} ");
            stringBuilder.AppendLine($"FROM (SELECT {stringBuilderParametersValues.ToString()}) as [c]");
            stringBuilder.AppendLine($"LEFT JOIN {trackingName.Quoted().ToString()} AS [side] ON {str1}");
            stringBuilder.AppendLine($"LEFT JOIN {tableName.Quoted().ToString()} AS [base] ON {str2}");

            stringBuilder.Append($"WHERE ({SqlManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, "[base]")} ");
            stringBuilder.AppendLine($"AND ([side].[timestamp] < @sync_min_timestamp OR [side].[update_scope_id] = @sync_scope_id)) ");
            stringBuilder.Append($"OR ({SqlManagementUtils.WhereColumnIsNull(this.TableDescription.PrimaryKeys, "[base]")} ");
            stringBuilder.AppendLine($"AND ([side].[timestamp] < @sync_min_timestamp OR [side].[timestamp] IS NULL)) ");
            stringBuilder.AppendLine($"OR @sync_force_write = 1;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("declare @rowcount int = @@rowcount;");

            stringBuilder.AppendLine($"UPDATE {trackingName.Quoted().ToString()} SET ");
            stringBuilder.AppendLine("[update_scope_id] = @sync_scope_id,");
            stringBuilder.AppendLine("[sync_row_is_tombstone] = 0,");
            stringBuilder.AppendLine("[last_change_datetime] = Getdate()");
            stringBuilder.AppendLine($"WHERE {SqlManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, "")}");
            stringBuilder.AppendLine($"AND @rowcount > 0");

            var cmdtext = stringBuilder.ToString();

            return cmdtext;
        }

        private string CreateUpdateCommand()
        {

            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();
            string empty = string.Empty;

            // Check if we have mutables columns
            var hasMutableColumns = this.TableDescription.GetMutableColumns(false).Any();

            var stringBuilder = new StringBuilder();

            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[p]", "[t]");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[changes]", "[base]");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[t]", "[side]");
            string str7 = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[p]", "[side]");

            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @dms_changed TABLE (");
            foreach (var c in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();

                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = this.sqlDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.GetDbType(), false, false, c.MaxLength, this.TableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var quotedColumnType = ParserName.Parse(sqlDbTypeString).Quoted().ToString();
                quotedColumnType += this.sqlDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.GetDbType(), false, false, c.MaxLength, c.Precision, c.Scale, this.TableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

                stringBuilder.Append($"{columnName} {quotedColumnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");

            var pkeyComma = " ";
            foreach (var primaryKeyColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(primaryKeyColumn).Quoted().ToString();
                stringBuilder.Append($"{pkeyComma}{columnName}");
                pkeyComma = ", ";
            }

            stringBuilder.AppendLine("));");
            stringBuilder.AppendLine();

            // Check if we have auto inc column
            if (this.TableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {tableName.Schema().Quoted().ToString()} ON;");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine(";WITH [changes] AS (");
            stringBuilder.Append("\tSELECT ");
            foreach (var c in this.TableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"[p].{columnName}, ");
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\t[side].[update_scope_id], [side].[timestamp], [side].[sync_row_is_tombstone]");
            stringBuilder.AppendLine($"\tFROM (SELECT ");
            stringBuilder.Append($"\t\t ");
            string comma = "";
            foreach (var c in this.TableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                var columnParameterName = ParserName.Parse(c).Unquoted().Normalized().ToString();

                stringBuilder.Append($"{comma}@{columnParameterName} as {columnName}");
                comma = ", ";
            }
            stringBuilder.AppendLine($") AS [p]");
            stringBuilder.Append($"\tLEFT JOIN {trackingName.Schema().Quoted().ToString()} [side] ON ");
            stringBuilder.AppendLine($"\t{str7}");
            stringBuilder.AppendLine($"\t)");

            stringBuilder.AppendLine($"MERGE {tableName.Schema().Quoted().ToString()} AS [base]");
            stringBuilder.AppendLine($"USING [changes] on {str5}");
            if (hasMutableColumns)
            {
                stringBuilder.AppendLine("WHEN MATCHED AND ([changes].[timestamp] <= @sync_min_timestamp OR [changes].[timestamp] IS NULL OR [changes].[update_scope_id] = @sync_scope_id OR @sync_force_write = 1) THEN");
                foreach (var mutableColumn in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
                {
                    var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                    stringBuilderArguments.Append(string.Concat(empty, columnName));
                    stringBuilderParameters.Append(string.Concat(empty, $"changes.{columnName}"));
                    empty = ", ";
                }
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"\tUPDATE SET");

                string strSeparator = "";
                foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false))
                {
                    var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                    stringBuilder.AppendLine($"\t{strSeparator}{columnName} = [changes].{columnName}");
                    strSeparator = ", ";
                }
            }

            stringBuilder.AppendLine("WHEN NOT MATCHED BY TARGET AND ([changes].[timestamp] <= @sync_min_timestamp OR [changes].[timestamp] IS NULL OR @sync_force_write = 1) THEN");


            stringBuilderArguments = new StringBuilder();
            stringBuilderParameters = new StringBuilder();
            empty = string.Empty;

            foreach (var mutableColumn in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();

                stringBuilderArguments.Append(string.Concat(empty, columnName));
                stringBuilderParameters.Append(string.Concat(empty, $"[changes].{columnName}"));
                empty = ", ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\tINSERT");
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()})");

            stringBuilder.AppendLine();
            stringBuilder.Append($"OUTPUT ");

            pkeyComma = " ";
            foreach (var primaryKeyColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(primaryKeyColumn).Quoted().ToString();
                stringBuilder.Append($"{pkeyComma}INSERTED.{columnName}");
                pkeyComma = ", ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"INTO @dms_changed; -- populates the temp table with successful PKs");
            stringBuilder.AppendLine();

            // Check if we have auto inc column
            if (this.TableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {tableName.Schema().Quoted().ToString()} ON;");
                stringBuilder.AppendLine();
            }


            stringBuilder.AppendLine("-- Since the update trigger is passed, we update the tracking table to reflect the real scope updater");
            stringBuilder.AppendLine("UPDATE [side] SET");
            stringBuilder.AppendLine("\t[update_scope_id] = @sync_scope_id,");
            stringBuilder.AppendLine("\t[sync_row_is_tombstone] = 0,");
            stringBuilder.AppendLine("\t[last_change_datetime] = GETUTCDATE()");
            stringBuilder.AppendLine($"FROM {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.AppendLine($"JOIN @dms_changed [t] on {str6}");
            stringBuilder.AppendLine();

            var commanString = stringBuilder.ToString();
            return commanString;
        }


        private string CreateUpdateMetadataCommand()
        {
            var stringBuilder = new StringBuilder();
            var pkeysForUpdate = new StringBuilder();

            var pkeySelectForInsert = new StringBuilder();
            var pkeyISelectForInsert = new StringBuilder();
            var pkeyAliasSelectForInsert = new StringBuilder();
            var pkeysLeftJoinForInsert = new StringBuilder();
            var pkeysIsNullForInsert = new StringBuilder();

            string and = string.Empty;
            string comma = string.Empty;
            foreach (var pkColumn in TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();

                pkeysForUpdate.Append($"{and}[side].{columnName} = @{parameterName}");

                pkeySelectForInsert.Append($"{comma}{columnName}");
                pkeyISelectForInsert.Append($"{comma}[i].{columnName}");
                pkeyAliasSelectForInsert.Append($"{comma}@{parameterName} as {columnName}");
                pkeysLeftJoinForInsert.Append($"{and}[side].{columnName} = [i].{columnName}");
                pkeysIsNullForInsert.Append($"{and}[side].{columnName} IS NULL");
                and = " AND ";
                comma = ", ";
            }


            stringBuilder.AppendLine($"UPDATE [side] SET ");
            stringBuilder.AppendLine($" [update_scope_id] = @sync_scope_id, ");
            stringBuilder.AppendLine($" [sync_row_is_tombstone] = @sync_row_is_tombstone, ");
            stringBuilder.AppendLine($" [last_change_datetime] = GETUTCDATE() ");
            stringBuilder.AppendLine($"FROM {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.Append($"WHERE ");
            stringBuilder.Append(pkeysForUpdate.ToString());
            stringBuilder.AppendLine($";");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"INSERT INTO {trackingName.Schema().Quoted().ToString()} (");
            stringBuilder.AppendLine(pkeySelectForInsert.ToString());
            stringBuilder.AppendLine(",[update_scope_id], [sync_row_is_tombstone],[last_change_datetime] )");
            stringBuilder.AppendLine($"SELECT {pkeyISelectForInsert.ToString()} ");
            stringBuilder.AppendLine($"   , i.sync_scope_id, i.sync_row_is_tombstone, i.UtcDate");
            stringBuilder.AppendLine("FROM (");
            stringBuilder.AppendLine($"  SELECT {pkeyAliasSelectForInsert}");
            stringBuilder.AppendLine($"          ,@sync_scope_id as sync_scope_id, @sync_row_is_tombstone as sync_row_is_tombstone, GETUTCDATE() as UtcDate) as i");
            stringBuilder.AppendLine($"LEFT JOIN  {trackingName.Schema().Quoted().ToString()} [side] ON {pkeysLeftJoinForInsert.ToString()} ");
            stringBuilder.AppendLine($"WHERE {pkeysIsNullForInsert.ToString()};");


            return stringBuilder.ToString();
        }

        private string CreateUpdateUntrackedRowsCommand()
        {
            var stringBuilder = new StringBuilder();
            var str1 = new StringBuilder();
            var str2 = new StringBuilder();
            var str3 = new StringBuilder();
            var str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[side]", "[base]");

            stringBuilder.AppendLine($"INSERT INTO {trackingName.Schema().Quoted().ToString()} (");


            var comma = "";
            foreach(var pkeyColumn in TableDescription.GetPrimaryKeysColumns())
            {
                var pkeyColumnName = ParserName.Parse(pkeyColumn).Quoted().ToString();

                str1.Append($"{comma}{pkeyColumnName}");
                str2.Append($"{comma}[base].{pkeyColumnName}");
                str3.Append($"{comma}[side].{pkeyColumnName}");

                comma = ", ";
            }
            stringBuilder.Append(str1.ToString());
            stringBuilder.AppendLine($", [update_scope_id], [sync_row_is_tombstone], [last_change_datetime]");
            stringBuilder.AppendLine($")");
            stringBuilder.Append($"SELECT ");
            stringBuilder.Append(str2.ToString());
            stringBuilder.AppendLine($", NULL, 0, GetUtcDate()");
            stringBuilder.AppendLine($"FROM {tableName.Schema().Quoted().ToString()} as [base] WHERE NOT EXISTS");
            stringBuilder.Append($"(SELECT ");
            stringBuilder.Append(str3.ToString());
            stringBuilder.AppendLine($" FROM {trackingName.Schema().Quoted().ToString()} as [side] ");
            stringBuilder.AppendLine($"WHERE {str4})");

            var r = stringBuilder.ToString();

            return r;

        }

    }
}
