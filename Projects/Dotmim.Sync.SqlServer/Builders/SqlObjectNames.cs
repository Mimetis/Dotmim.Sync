using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.SqlServer.Builders
{
    /// <summary>
    /// Sql Object Names.
    /// </summary>
    public class SqlObjectNames
    {
        /// <summary>
        /// Gets the left quote character.
        /// </summary>
        public const char LeftQuote = '[';

        /// <summary>
        /// Gets the right quote character.
        /// </summary>
        public const char RightQuote = ']';

        internal const string InsertTriggerName = "[{0}].[{1}insert_trigger]";
        internal const string UpdateTriggerName = "[{0}].[{1}update_trigger]";
        internal const string DeleteTriggerName = "[{0}].[{1}delete_trigger]";

        internal const string SelectChangesProcName = "[{0}].[{1}{2}changes]";
        internal const string SelectChangesProcNameWithFilters = "[{0}].[{1}{2}{3}changes]";

        internal const string InitializeChangesProcName = "[{0}].[{1}{2}initialize]";
        internal const string InitializeChangesProcNameWithFilters = "[{0}].[{1}{2}{3}initialize]";

        internal const string SelectRowProcName = "[{0}].[{1}{2}selectrow]";

        internal const string InsertProcName = "[{0}].[{1}{2}insert]";
        internal const string UpdateProcName = "[{0}].[{1}{2}update]";
        internal const string DeleteProcName = "[{0}].[{1}{2}delete]";

        internal const string ResetMetadataProcName = "[{0}].[{1}{2}reset]";

        internal const string BulkTableTypeName = "[{0}].[{1}{2}BulkType]";

        internal const string BulkInsertProcName = "[{0}].[{1}{2}bulkinsert]";
        internal const string BulkUpdateProcName = "[{0}].[{1}{2}bulkupdate]";
        internal const string BulkDeleteProcName = "[{0}].[{1}{2}bulkdelete]";

        internal const string DisableConstraintsText = "ALTER TABLE {0} NOCHECK CONSTRAINT ALL";
        internal const string EnableConstraintsText = "ALTER TABLE {0} CHECK CONSTRAINT ALL";
        internal const string DeleteMetadataText = "DELETE [side] FROM {0} [side] WHERE [side].[timestamp] <= @sync_row_timestamp";

        /// <summary>
        /// Gets the table description.
        /// </summary>
        public SyncTable TableDescription { get; }

        /// <summary>
        /// Gets the scope info.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <summary>
        /// Gets the parsed tracking table name, wihtout any quotes characters.
        /// </summary>
        public string TrackingTableName { get; private set; }

        /// <summary>
        /// Gets the parsed normalized tracking table full name.
        /// </summary>
        public string TrackingTableNormalizedFullName { get; private set; }

        /// <summary>
        /// Gets the parsed normalized tracking table short name.
        /// </summary>
        public string TrackingTableNormalizedShortName { get; private set; }

        /// <summary>
        /// Gets the parsed quoted tracking table full name.
        /// </summary>
        public string TrackingTableQuotedFullName { get; private set; }

        /// <summary>
        /// Gets the parsed quoted tracking table short name.
        /// </summary>
        public string TrackingTableQuotedShortName { get; private set; }

        /// <summary>
        /// Gets the parsed tracking table schema name. if empty, "dbo" is returned.
        /// </summary>
        public string TrackingTableSchemaName { get; private set; }

        /// <summary>
        /// Gets the parsed table name, without any quotes characters.
        /// </summary>
        public string TableName { get; private set; }

        /// <summary>
        /// Gets the parsed normalized table full name (with schema, if any).
        /// </summary>
        public string TableNormalizedFullName { get; private set; }

        /// <summary>
        /// Gets the parsed normalized table short name (without schema, if any).
        /// </summary>
        public string TableNormalizedShortName { get; private set; }

        /// <summary>
        /// Gets the parsed quoted table full name (with schema, if any).
        /// </summary>
        public string TableQuotedFullName { get; private set; }

        /// <summary>
        /// Gets the parsed quoted table short name (without schema, if any).
        /// </summary>
        public string TableQuotedShortName { get; private set; }

        /// <summary>
        /// Gets the parsed table schema name. if empty, "dbo" is returned.
        /// </summary>
        public string TableSchemaName { get; private set; }

        /// <summary>
        /// Get the command name for a stored procedure type.
        /// </summary>
        public string GetStoredProcedureCommandName(DbStoredProcedureType storedProcedureType, SyncFilter filter = null)
        {
            var scopeNameWithoutDefaultScope = this.ScopeInfo.Name == SyncOptions.DefaultScopeName ? string.Empty : $"{this.ScopeInfo.Name}_";
            var storedProcedureNormalizedName = $"{this.ScopeInfo.Setup?.StoredProceduresPrefix}{this.TableNormalizedShortName}{this.ScopeInfo.Setup?.StoredProceduresSuffix}_";

            return storedProcedureType switch
            {
                DbStoredProcedureType.SelectChanges => string.Format(SelectChangesProcName, this.TableSchemaName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbStoredProcedureType.SelectChangesWithFilters => string.Format(SelectChangesProcNameWithFilters, this.TableSchemaName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope, filter.GetFilterName()),
                DbStoredProcedureType.SelectInitializedChanges => string.Format(InitializeChangesProcName, this.TableSchemaName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbStoredProcedureType.SelectInitializedChangesWithFilters => string.Format(InitializeChangesProcNameWithFilters, this.TableSchemaName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope, filter.GetFilterName()),
                DbStoredProcedureType.SelectRow => string.Format(SelectRowProcName, this.TableSchemaName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbStoredProcedureType.UpdateRow => string.Format(UpdateProcName, this.TableSchemaName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbStoredProcedureType.DeleteRow => string.Format(DeleteProcName, this.TableSchemaName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbStoredProcedureType.BulkUpdateRows => string.Format(BulkUpdateProcName, this.TableSchemaName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbStoredProcedureType.BulkDeleteRows => string.Format(BulkDeleteProcName, this.TableSchemaName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbStoredProcedureType.Reset => string.Format(ResetMetadataProcName, this.TableSchemaName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbStoredProcedureType.BulkTableType => string.Format(BulkTableTypeName, this.TableSchemaName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                _ => null,
            };
        }

        /// <summary>
        /// Get the trigger name for a trigger type.
        /// </summary>
        public string GetTriggerCommandName(DbTriggerType objectType)
        {
            var triggerNormalizedName = $"{this.ScopeInfo.Setup?.TriggersPrefix}{this.TableNormalizedShortName}{this.ScopeInfo.Setup?.TriggersSuffix}_";

            return objectType switch
            {
                DbTriggerType.Update => string.Format(UpdateTriggerName, this.TableSchemaName, triggerNormalizedName),
                DbTriggerType.Insert => string.Format(InsertTriggerName, this.TableSchemaName, triggerNormalizedName),
                DbTriggerType.Delete => string.Format(DeleteTriggerName, this.TableSchemaName, triggerNormalizedName),
                _ => null,
            };
        }

        /// <summary>
        /// Get a command string from the command type.
        /// </summary>
        public string GetCommandName(DbCommandType commandType, SyncFilter filter = null)
        {
            var scopeNameWithoutDefaultScope = this.ScopeInfo.Name == SyncOptions.DefaultScopeName ? string.Empty : $"{this.ScopeInfo.Name}_";

            //-------------------------------------------------
            // Stored procedures & Triggers
            var storedProcedureNormalizedName = $"{this.ScopeInfo.Setup?.StoredProceduresPrefix}{this.TableNormalizedShortName}{this.ScopeInfo.Setup?.StoredProceduresSuffix}_";
            var triggerNormalizedName = $"{this.ScopeInfo.Setup?.TriggersPrefix}{this.TableNormalizedShortName}{this.ScopeInfo.Setup?.TriggersSuffix}_";

            return commandType switch
            {
                DbCommandType.SelectChanges => string.Format(SelectChangesProcName, this.TableSchemaName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbCommandType.SelectInitializedChanges => string.Format(InitializeChangesProcName, this.TableSchemaName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbCommandType.SelectInitializedChangesWithFilters => string.Format(InitializeChangesProcNameWithFilters, this.TableSchemaName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope, filter.GetFilterName()),
                DbCommandType.SelectChangesWithFilters => string.Format(SelectChangesProcNameWithFilters, this.TableSchemaName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope, filter.GetFilterName()),
                DbCommandType.SelectRow => this.CreateSelectRowCommand(),
                DbCommandType.UpdateRow or DbCommandType.InsertRow => string.Format(UpdateProcName, this.TableSchemaName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbCommandType.DeleteRow => string.Format(DeleteProcName, this.TableSchemaName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbCommandType.DisableConstraints => string.Format(DisableConstraintsText, this.TableQuotedFullName),
                DbCommandType.EnableConstraints => string.Format(EnableConstraintsText, this.TableQuotedFullName),
                DbCommandType.DeleteMetadata => string.Format(DeleteMetadataText, this.TrackingTableQuotedFullName),
                DbCommandType.UpdateMetadata => this.CreateUpdateMetadataCommand(),
                DbCommandType.SelectMetadata => this.CreateSelectMetadataCommand(),
                DbCommandType.InsertTrigger => string.Format(InsertTriggerName, this.TableSchemaName, triggerNormalizedName),
                DbCommandType.UpdateTrigger => string.Format(UpdateTriggerName, this.TableSchemaName, triggerNormalizedName),
                DbCommandType.DeleteTrigger => string.Format(DeleteTriggerName, this.TableSchemaName, triggerNormalizedName),
                DbCommandType.UpdateRows or DbCommandType.InsertRows => string.Format(BulkUpdateProcName, this.TableSchemaName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbCommandType.DeleteRows => string.Format(BulkDeleteProcName, this.TableSchemaName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbCommandType.BulkTableType => string.Format(BulkTableTypeName, this.TableSchemaName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbCommandType.UpdateUntrackedRows => this.CreateUpdateUntrackedRowsCommand(),
                DbCommandType.Reset => this.CreateResetCommand(),
                _ => null,
            };
        }

        /// <inheritdoc cref="SqlObjectNames"/>
        public SqlObjectNames(SyncTable tableDescription, ScopeInfo scopeInfo)
        {
            this.TableDescription = tableDescription;
            this.ScopeInfo = scopeInfo;

            //-------------------------------------------------
            // set table names
            var tableParser = new TableParser(this.TableDescription.GetFullName(), LeftQuote, RightQuote);

            this.TableName = tableParser.TableName;
            this.TableNormalizedFullName = tableParser.NormalizedFullName;
            this.TableNormalizedShortName = tableParser.NormalizedShortName;
            this.TableQuotedFullName = tableParser.QuotedFullName;
            this.TableQuotedShortName = tableParser.QuotedShortName;
            this.TableSchemaName = SqlManagementUtils.GetUnquotedSqlSchemaName(tableParser);

            //-------------------------------------------------
            // define tracking table name with prefix and suffix.
            // if no pref / suf, use default value
            var trakingTableNameString = string.IsNullOrEmpty(this.ScopeInfo.Setup?.TrackingTablesPrefix) && string.IsNullOrEmpty(this.ScopeInfo.Setup?.TrackingTablesSuffix)
                ? $"{this.TableDescription.TableName}_tracking"
                : $"{this.ScopeInfo.Setup?.TrackingTablesPrefix}{this.TableDescription.TableName}{this.ScopeInfo.Setup?.TrackingTablesSuffix}";

            if (!string.IsNullOrEmpty(this.TableDescription.SchemaName))
                trakingTableNameString = $"{this.TableDescription.SchemaName}.{trakingTableNameString}";

            // Parse
            var trackingTableParser = new TableParser(trakingTableNameString, LeftQuote, RightQuote);

            // set the tracking table names
            this.TrackingTableName = trackingTableParser.TableName;
            this.TrackingTableNormalizedFullName = trackingTableParser.NormalizedFullName;
            this.TrackingTableNormalizedShortName = trackingTableParser.NormalizedShortName;
            this.TrackingTableQuotedFullName = trackingTableParser.QuotedFullName;
            this.TrackingTableQuotedShortName = trackingTableParser.QuotedShortName;
            this.TrackingTableSchemaName = SqlManagementUtils.GetUnquotedSqlSchemaName(trackingTableParser);
        }

        private string CreateSelectMetadataCommand()
        {
            var stringBuilder = new StringBuilder();
            var pkeysSelect = new StringBuilder();
            var pkeysWhere = new StringBuilder();

            string and = string.Empty;
            string comma = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, LeftQuote, RightQuote);
                pkeysSelect.Append($"{comma}[side].{columnParser.QuotedShortName}");

                pkeysWhere.Append($"{and}[side].{columnParser.QuotedShortName} = @{columnParser.NormalizedShortName}");

                and = " AND ";
                comma = ", ";
            }

            stringBuilder.AppendLine($"SELECT {pkeysSelect}, [side].[update_scope_id], [side].[timestamp_bigint] as [timestamp], [side].[sync_row_is_tombstone]");
            stringBuilder.AppendLine($"FROM {this.TrackingTableQuotedFullName} [side]");
            stringBuilder.AppendLine($"WHERE {pkeysWhere}");

            return stringBuilder.ToString();
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
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, LeftQuote, RightQuote);

                pkeysForUpdate.Append($"{and}[side].{columnParser.QuotedShortName} = @{columnParser.NormalizedShortName}");

                pkeySelectForInsert.Append($"{comma}{columnParser.QuotedShortName}");
                pkeyISelectForInsert.Append($"{comma}[i].{columnParser.QuotedShortName}");
                pkeyAliasSelectForInsert.Append($"{comma}@{columnParser.NormalizedShortName} as {columnParser.QuotedShortName}");
                pkeysLeftJoinForInsert.Append($"{and}[side].{columnParser.QuotedShortName} = [i].{columnParser.QuotedShortName}");
                pkeysIsNullForInsert.Append($"{and}[side].{columnParser.QuotedShortName} IS NULL");
                and = " AND ";
                comma = ", ";
            }

            stringBuilder.AppendLine($"UPDATE [side] SET ");
            stringBuilder.AppendLine($" [update_scope_id] = @sync_scope_id, ");
            stringBuilder.AppendLine($" [sync_row_is_tombstone] = @sync_row_is_tombstone, ");
            stringBuilder.AppendLine($" [last_change_datetime] = GETUTCDATE() ");
            stringBuilder.AppendLine($"FROM {this.TrackingTableQuotedFullName} [side]");
            stringBuilder.Append($"WHERE ");
            stringBuilder.Append(pkeysForUpdate);
            stringBuilder.AppendLine($";");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"INSERT INTO {this.TrackingTableQuotedFullName} (");
            stringBuilder.AppendLine(pkeySelectForInsert.ToString());
            stringBuilder.AppendLine(",[update_scope_id], [sync_row_is_tombstone],[last_change_datetime] )");
            stringBuilder.AppendLine($"SELECT {pkeyISelectForInsert} ");
            stringBuilder.AppendLine($"   , i.sync_scope_id, i.sync_row_is_tombstone, i.UtcDate");
            stringBuilder.AppendLine("FROM (");
            stringBuilder.AppendLine($"  SELECT {pkeyAliasSelectForInsert}");
            stringBuilder.AppendLine($"          ,@sync_scope_id as sync_scope_id, @sync_row_is_tombstone as sync_row_is_tombstone, GETUTCDATE() as UtcDate) as i");
            stringBuilder.AppendLine($"LEFT JOIN  {this.TrackingTableQuotedFullName} [side] ON {pkeysLeftJoinForInsert} ");
            stringBuilder.AppendLine($"WHERE {pkeysIsNullForInsert};");

            return stringBuilder.ToString();
        }

        private string CreateUpdateUntrackedRowsCommand()
        {
            var stringBuilder = new StringBuilder();
            var str1 = new StringBuilder();
            var str2 = new StringBuilder();
            var str3 = new StringBuilder();
            var str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[side]", "[base]");

            stringBuilder.AppendLine($"INSERT INTO {this.TrackingTableQuotedFullName} (");

            var comma = string.Empty;
            foreach (var pkeyColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkeyColumn.ColumnName, LeftQuote, RightQuote);

                str1.Append($"{comma}{columnParser.QuotedShortName}");
                str2.Append($"{comma}[base].{columnParser.QuotedShortName}");
                str3.Append($"{comma}[side].{columnParser.QuotedShortName}");

                comma = ", ";
            }

            stringBuilder.Append(str1);
            stringBuilder.AppendLine($", [update_scope_id], [sync_row_is_tombstone], [last_change_datetime]");
            stringBuilder.AppendLine($")");
            stringBuilder.Append($"SELECT ");
            stringBuilder.Append(str2);
            stringBuilder.AppendLine($", NULL, 0, GetUtcDate()");
            stringBuilder.AppendLine($"FROM {this.TableQuotedFullName} as [base] WHERE NOT EXISTS");
            stringBuilder.Append($"(SELECT ");
            stringBuilder.Append(str3);
            stringBuilder.AppendLine($" FROM {this.TrackingTableQuotedFullName} as [side] ");
            stringBuilder.AppendLine($"WHERE {str4})");

            var r = stringBuilder.ToString();

            return r;
        }

        private string CreateSelectRowCommand()
        {
            var stringBuilder = new StringBuilder("SELECT ");
            stringBuilder.AppendLine();
            var stringBuilder1 = new StringBuilder();
            string empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, LeftQuote, RightQuote);

                stringBuilder1.Append($"{empty}[side].{columnParser.QuotedShortName} = @{columnParser.NormalizedShortName}");
                empty = " AND ";
            }

            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, LeftQuote, RightQuote);

                var isPrimaryKey = this.TableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.AppendLine($"\t[side].{columnParser.QuotedShortName}, ");
                else
                    stringBuilder.AppendLine($"\t[base].{columnParser.QuotedShortName}, ");
            }

            stringBuilder.AppendLine($"\t[side].[sync_row_is_tombstone] as [sync_row_is_tombstone], ");
            stringBuilder.AppendLine($"\t[side].[update_scope_id] as [sync_update_scope_id]");

            stringBuilder.AppendLine($"FROM {this.TableQuotedFullName} [base]");
            stringBuilder.AppendLine($"RIGHT JOIN {this.TrackingTableQuotedFullName} [side] ON");

            string str = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, LeftQuote, RightQuote);
                stringBuilder.Append($"{str}[base].{columnParser.QuotedShortName} = [side].{columnParser.QuotedShortName}");
                str = " AND ";
            }

            stringBuilder.AppendLine();
            stringBuilder.Append(string.Concat("WHERE ", stringBuilder1.ToString()));
            return stringBuilder.ToString();
        }

        private string CreateResetCommand()
        {
            var updTriggerName = this.GetCommandName(DbCommandType.UpdateTrigger);
            var delTriggerName = this.GetCommandName(DbCommandType.DeleteTrigger);
            var insTriggerName = this.GetCommandName(DbCommandType.InsertTrigger);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"SET @sync_row_count = 0;");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"DISABLE TRIGGER {updTriggerName} ON {this.TableQuotedFullName};");
            stringBuilder.AppendLine($"DISABLE TRIGGER {insTriggerName} ON {this.TableQuotedFullName};");
            stringBuilder.AppendLine($"DISABLE TRIGGER {delTriggerName} ON {this.TableQuotedFullName};");

            stringBuilder.AppendLine($"DELETE FROM {this.TableQuotedFullName};");
            stringBuilder.AppendLine($"DELETE FROM {this.TrackingTableQuotedFullName};");

            stringBuilder.AppendLine($"ENABLE TRIGGER {updTriggerName} ON {this.TableQuotedFullName};");
            stringBuilder.AppendLine($"ENABLE TRIGGER {insTriggerName} ON {this.TableQuotedFullName};");
            stringBuilder.AppendLine($"ENABLE TRIGGER {delTriggerName} ON {this.TableQuotedFullName};");

            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("SET @sync_row_count = @@ROWCOUNT;"));

            return stringBuilder.ToString();
        }
    }
}