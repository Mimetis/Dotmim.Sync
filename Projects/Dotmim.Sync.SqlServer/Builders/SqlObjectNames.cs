using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlObjectNames
    {

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

        private readonly ParserName tableName;
        private readonly ParserName trackingName;

        private Dictionary<DbStoredProcedureType, string> storedProceduresNames = [];
        private Dictionary<DbTriggerType, string> triggersNames = [];
        private Dictionary<DbCommandType, string> commandNames = [];

        public SyncTable TableDescription { get; }

        public SyncSetup Setup { get; }

        public string ScopeName { get; }

        public void AddStoredProcedureName(DbStoredProcedureType objectType, string name)
        {
            if (this.storedProceduresNames.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            this.storedProceduresNames.Add(objectType, name);
        }

        public string GetStoredProcedureCommandName(DbStoredProcedureType storedProcedureType, SyncFilter filter = null)
        {
            if (!this.storedProceduresNames.TryGetValue(storedProcedureType, out var commandName))
                throw new Exception("Yous should provide a value for all DbCommandName");

            // concat filter name
            if (filter != null && (storedProcedureType == DbStoredProcedureType.SelectChangesWithFilters || storedProcedureType == DbStoredProcedureType.SelectInitializedChangesWithFilters))
                commandName = string.Format(commandName, filter.GetFilterName());

            return commandName;
        }

        public void AddTriggerName(DbTriggerType objectType, string name)
        {
            if (this.triggersNames.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            this.triggersNames.Add(objectType, name);
        }

        public string GetTriggerCommandName(DbTriggerType objectType, SyncFilter filter = null)
        {
            if (!this.triggersNames.TryGetValue(objectType, out var commandName))
                throw new Exception("Yous should provide a value for all DbCommandName");

            // concat filter name
            if (filter != null)
                commandName = string.Format(commandName, filter.GetFilterName());

            return commandName;
        }

        public void AddCommandName(DbCommandType objectType, string name)
        {
            if (this.commandNames.ContainsKey(objectType))
                throw new Exception($"Yous can't add an objectType ${objectType} multiple times");

            this.commandNames.Add(objectType, name);
        }

        public string GetCommandName(DbCommandType objectType, SyncFilter filter = null)
        {
            if (!this.commandNames.TryGetValue(objectType, out var commandName))
                throw new Exception("Yous should provide a value for all DbCommandName");

            // concat filter name
            if (filter != null)
                commandName = string.Format(commandName, filter.GetFilterName());

            return commandName;
        }

        public SqlObjectNames(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup, string scopeName)
        {
            this.TableDescription = tableDescription;
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.Setup = setup;
            this.ScopeName = scopeName;
            this.SetDefaultNames();
        }

        /// <summary>
        /// Set the default stored procedures names.
        /// </summary>
        private void SetDefaultNames()
        {
            var pref = this.Setup?.StoredProceduresPrefix;
            var suf = this.Setup?.StoredProceduresSuffix;
            var tpref = this.Setup?.TriggersPrefix;
            var tsuf = this.Setup?.TriggersSuffix;

            var tableName = ParserName.Parse(this.TableDescription);

            var scopeNameWithoutDefaultScope = this.ScopeName == SyncOptions.DefaultScopeName ? string.Empty : $"{this.ScopeName}_";

            var schema = string.IsNullOrEmpty(tableName.SchemaName) ? "dbo" : tableName.SchemaName;

            var storedProcedureName = $"{pref}{tableName.Unquoted().Normalized()}{suf}_";
            var triggerName = $"{tpref}{tableName.Unquoted().Normalized()}{tsuf}_";

            this.AddStoredProcedureName(DbStoredProcedureType.SelectChanges, string.Format(SelectChangesProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.SelectChangesWithFilters, string.Format(SelectChangesProcNameWithFilters, schema, storedProcedureName, scopeNameWithoutDefaultScope, "{0}_"));

            this.AddStoredProcedureName(DbStoredProcedureType.SelectInitializedChanges, string.Format(InitializeChangesProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.SelectInitializedChangesWithFilters, string.Format(InitializeChangesProcNameWithFilters, schema, storedProcedureName, scopeNameWithoutDefaultScope, "{0}_"));

            this.AddStoredProcedureName(DbStoredProcedureType.SelectRow, string.Format(SelectRowProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.UpdateRow, string.Format(UpdateProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.DeleteRow, string.Format(DeleteProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.Reset, string.Format(ResetMetadataProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));

            this.AddTriggerName(DbTriggerType.Insert, string.Format(InsertTriggerName, schema, triggerName));
            this.AddTriggerName(DbTriggerType.Update, string.Format(UpdateTriggerName, schema, triggerName));
            this.AddTriggerName(DbTriggerType.Delete, string.Format(DeleteTriggerName, schema, triggerName));

            this.AddStoredProcedureName(DbStoredProcedureType.BulkTableType, string.Format(BulkTableTypeName, schema, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.BulkUpdateRows, string.Format(BulkUpdateProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.BulkDeleteRows, string.Format(BulkDeleteProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));

            this.AddCommandName(DbCommandType.DisableConstraints, string.Format(DisableConstraintsText, ParserName.Parse(this.TableDescription).Schema().Quoted().ToString()));
            this.AddCommandName(DbCommandType.EnableConstraints, string.Format(EnableConstraintsText, ParserName.Parse(this.TableDescription).Schema().Quoted().ToString()));

            this.AddCommandName(DbCommandType.DeleteMetadata, string.Format(DeleteMetadataText, this.trackingName.Schema().Quoted().ToString()));

            this.AddCommandName(DbCommandType.UpdateUntrackedRows, this.CreateUpdateUntrackedRowsCommand());
            this.AddCommandName(DbCommandType.SelectMetadata, this.CreateSelectMetadataCommand());
            this.AddCommandName(DbCommandType.UpdateMetadata, this.CreateUpdateMetadataCommand());
            this.AddCommandName(DbCommandType.SelectRow, this.CreateSelectRowCommand());
            this.AddCommandName(DbCommandType.Reset, this.CreateResetCommand());
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
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();
                pkeysSelect.Append($"{comma}[side].{columnName}");

                pkeysWhere.Append($"{and}[side].{columnName} = @{parameterName}");

                and = " AND ";
                comma = ", ";
            }

            stringBuilder.AppendLine($"SELECT {pkeysSelect}, [side].[update_scope_id], [side].[timestamp_bigint] as [timestamp], [side].[sync_row_is_tombstone]");
            stringBuilder.AppendLine($"FROM {this.trackingName.Schema().Quoted()} [side]");
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
            stringBuilder.AppendLine($"FROM {this.trackingName.Schema().Quoted()} [side]");
            stringBuilder.Append($"WHERE ");
            stringBuilder.Append(pkeysForUpdate);
            stringBuilder.AppendLine($";");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"INSERT INTO {this.trackingName.Schema().Quoted()} (");
            stringBuilder.AppendLine(pkeySelectForInsert.ToString());
            stringBuilder.AppendLine(",[update_scope_id], [sync_row_is_tombstone],[last_change_datetime] )");
            stringBuilder.AppendLine($"SELECT {pkeyISelectForInsert} ");
            stringBuilder.AppendLine($"   , i.sync_scope_id, i.sync_row_is_tombstone, i.UtcDate");
            stringBuilder.AppendLine("FROM (");
            stringBuilder.AppendLine($"  SELECT {pkeyAliasSelectForInsert}");
            stringBuilder.AppendLine($"          ,@sync_scope_id as sync_scope_id, @sync_row_is_tombstone as sync_row_is_tombstone, GETUTCDATE() as UtcDate) as i");
            stringBuilder.AppendLine($"LEFT JOIN  {this.trackingName.Schema().Quoted()} [side] ON {pkeysLeftJoinForInsert} ");
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

            stringBuilder.AppendLine($"INSERT INTO {this.trackingName.Schema().Quoted()} (");

            var comma = string.Empty;
            foreach (var pkeyColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var pkeyColumnName = ParserName.Parse(pkeyColumn).Quoted().ToString();

                str1.Append($"{comma}{pkeyColumnName}");
                str2.Append($"{comma}[base].{pkeyColumnName}");
                str3.Append($"{comma}[side].{pkeyColumnName}");

                comma = ", ";
            }

            stringBuilder.Append(str1);
            stringBuilder.AppendLine($", [update_scope_id], [sync_row_is_tombstone], [last_change_datetime]");
            stringBuilder.AppendLine($")");
            stringBuilder.Append($"SELECT ");
            stringBuilder.Append(str2);
            stringBuilder.AppendLine($", NULL, 0, GetUtcDate()");
            stringBuilder.AppendLine($"FROM {this.tableName.Schema().Quoted()} as [base] WHERE NOT EXISTS");
            stringBuilder.Append($"(SELECT ");
            stringBuilder.Append(str3);
            stringBuilder.AppendLine($" FROM {this.trackingName.Schema().Quoted()} as [side] ");
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
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();

                stringBuilder1.Append($"{empty}[side].{columnName} = @{parameterName}");
                empty = " AND ";
            }

            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();

                var isPrimaryKey = this.TableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.AppendLine($"\t[side].{columnName}, ");
                else
                    stringBuilder.AppendLine($"\t[base].{columnName}, ");
            }

            stringBuilder.AppendLine($"\t[side].[sync_row_is_tombstone] as [sync_row_is_tombstone], ");
            stringBuilder.AppendLine($"\t[side].[update_scope_id] as [sync_update_scope_id]");

            stringBuilder.AppendLine($"FROM {this.tableName.Schema().Quoted()} [base]");
            stringBuilder.AppendLine($"RIGHT JOIN {this.trackingName.Schema().Quoted()} [side] ON");

            string str = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.Append($"{str}[base].{columnName} = [side].{columnName}");
                str = " AND ";
            }

            stringBuilder.AppendLine();
            stringBuilder.Append(string.Concat("WHERE ", stringBuilder1.ToString()));
            return stringBuilder.ToString();
        }

        private string CreateResetCommand()
        {
            var updTriggerName = this.GetTriggerCommandName(DbTriggerType.Update);
            var delTriggerName = this.GetTriggerCommandName(DbTriggerType.Delete);
            var insTriggerName = this.GetTriggerCommandName(DbTriggerType.Insert);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"SET @sync_row_count = 0;");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"DISABLE TRIGGER {updTriggerName} ON {this.tableName.Schema().Quoted()};");
            stringBuilder.AppendLine($"DISABLE TRIGGER {insTriggerName} ON {this.tableName.Schema().Quoted()};");
            stringBuilder.AppendLine($"DISABLE TRIGGER {delTriggerName} ON {this.tableName.Schema().Quoted()};");

            stringBuilder.AppendLine($"DELETE FROM {this.tableName.Schema().Quoted()};");
            stringBuilder.AppendLine($"DELETE FROM {this.trackingName.Schema().Quoted()};");

            stringBuilder.AppendLine($"ENABLE TRIGGER {updTriggerName} ON {this.tableName.Schema().Quoted()};");
            stringBuilder.AppendLine($"ENABLE TRIGGER {insTriggerName} ON {this.tableName.Schema().Quoted()};");
            stringBuilder.AppendLine($"ENABLE TRIGGER {delTriggerName} ON {this.tableName.Schema().Quoted()};");

            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("SET @sync_row_count = @@ROWCOUNT;"));

            return stringBuilder.ToString();
        }
    }
}