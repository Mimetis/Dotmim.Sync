﻿using Dotmim.Sync.Builders;
using Dotmim.Sync.SqlServer.Manager;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlObjectNames
    {
        internal const string insertTriggerName = "[{0}].[{1}insert_trigger]";
        internal const string updateTriggerName = "[{0}].[{1}update_trigger]";
        internal const string deleteTriggerName = "[{0}].[{1}delete_trigger]";

        internal const string selectChangesProcName = "[{0}].[{1}{2}changes]";
        internal const string selectChangesProcNameWithFilters = "[{0}].[{1}{2}{3}changes]";

        internal const string initializeChangesProcName = "[{0}].[{1}{2}initialize]";
        internal const string initializeChangesProcNameWithFilters = "[{0}].[{1}{2}{3}initialize]";

        internal const string selectRowProcName = "[{0}].[{1}{2}selectrow]";

        internal const string insertProcName = "[{0}].[{1}{2}insert]";
        internal const string updateProcName = "[{0}].[{1}{2}update]";
        internal const string deleteProcName = "[{0}].[{1}{2}delete]";

        internal const string deleteMetadataProcName = "[{0}].[{1}{2}deletemetadata]";

        internal const string resetMetadataProcName = "[{0}].[{1}{2}reset]";

        internal const string bulkTableTypeName = "[{0}].[{1}{2}BulkType]";

        internal const string bulkInsertProcName = "[{0}].[{1}{2}bulkinsert]";
        internal const string bulkUpdateProcName = "[{0}].[{1}{2}bulkupdate]";
        internal const string bulkDeleteProcName = "[{0}].[{1}{2}bulkdelete]";

        internal const string disableConstraintsText = "ALTER TABLE {0} NOCHECK CONSTRAINT ALL";
        internal const string enableConstraintsText = "ALTER TABLE {0} CHECK CONSTRAINT ALL";
        private readonly ParserName tableName;
        private readonly ParserName trackingName;

        Dictionary<DbStoredProcedureType, string> storedProceduresNames = new();
        Dictionary<DbTriggerType, string> triggersNames = new();
        Dictionary<DbCommandType, string> commandNames = new();
        private SqlDbMetadata sqlDbMetadata;

        public SyncTable TableDescription { get; }
        public SyncSetup Setup { get; }
        public string ScopeName { get; }

        public void AddStoredProcedureName(DbStoredProcedureType objectType, string name)
        {
            if (storedProceduresNames.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            storedProceduresNames.Add(objectType, name);
        }
        public string GetStoredProcedureCommandName(DbStoredProcedureType storedProcedureType, SyncFilter filter = null)
        {
            if (!storedProceduresNames.ContainsKey(storedProcedureType))
                throw new Exception("Yous should provide a value for all DbCommandName");

            var commandName = storedProceduresNames[storedProcedureType];

            // concat filter name
            if (filter != null && (storedProcedureType == DbStoredProcedureType.SelectChangesWithFilters || storedProcedureType == DbStoredProcedureType.SelectInitializedChangesWithFilters))
                commandName = string.Format(commandName, filter.GetFilterName());

            return commandName;
        }

        public void AddTriggerName(DbTriggerType objectType, string name)
        {
            if (triggersNames.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            triggersNames.Add(objectType, name);
        }
        public string GetTriggerCommandName(DbTriggerType objectType, SyncFilter filter = null)
        {
            if (!triggersNames.ContainsKey(objectType))
                throw new Exception("Yous should provide a value for all DbCommandName");

            var commandName = triggersNames[objectType];

            // concat filter name
            if (filter != null)
                commandName = string.Format(commandName, filter.GetFilterName());

            return commandName;
        }
        public void AddCommandName(DbCommandType objectType, string name)
        {
            if (commandNames.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            commandNames.Add(objectType, name);
        }
        public string GetCommandName(DbCommandType objectType, SyncFilter filter = null)
        {
            if (!commandNames.ContainsKey(objectType))
                throw new Exception("Yous should provide a value for all DbCommandName");

            var commandName = commandNames[objectType];

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
            this.sqlDbMetadata = new SqlDbMetadata();
        }

        /// <summary>
        /// Set the default stored procedures names
        /// </summary>
        private void SetDefaultNames()
        {
            var pref = this.Setup?.StoredProceduresPrefix;
            var suf = this.Setup?.StoredProceduresSuffix;
            var tpref = this.Setup?.TriggersPrefix;
            var tsuf = this.Setup?.TriggersSuffix;

            var tableName = ParserName.Parse(TableDescription);

            var scopeNameWithoutDefaultScope = ScopeName == SyncOptions.DefaultScopeName ? "" : $"{ScopeName}_";

            var schema = string.IsNullOrEmpty(tableName.SchemaName) ? "dbo" : tableName.SchemaName;

            var storedProcedureName = $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}_";
            var triggerName = $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}_";

            this.AddStoredProcedureName(DbStoredProcedureType.SelectChanges, string.Format(selectChangesProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.SelectChangesWithFilters, string.Format(selectChangesProcNameWithFilters, schema, storedProcedureName, scopeNameWithoutDefaultScope, "{0}_"));

            this.AddStoredProcedureName(DbStoredProcedureType.SelectInitializedChanges, string.Format(initializeChangesProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.SelectInitializedChangesWithFilters, string.Format(initializeChangesProcNameWithFilters, schema, storedProcedureName, scopeNameWithoutDefaultScope, "{0}_"));

            this.AddStoredProcedureName(DbStoredProcedureType.SelectRow, string.Format(selectRowProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.UpdateRow, string.Format(updateProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.DeleteRow, string.Format(deleteProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.DeleteMetadata, string.Format(deleteMetadataProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.Reset, string.Format(resetMetadataProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));

            this.AddTriggerName(DbTriggerType.Insert, string.Format(insertTriggerName, schema, triggerName));
            this.AddTriggerName(DbTriggerType.Update, string.Format(updateTriggerName, schema, triggerName));
            this.AddTriggerName(DbTriggerType.Delete, string.Format(deleteTriggerName, schema, triggerName));

            this.AddStoredProcedureName(DbStoredProcedureType.BulkTableType, string.Format(bulkTableTypeName, schema, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.BulkUpdateRows, string.Format(bulkUpdateProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.BulkDeleteRows, string.Format(bulkDeleteProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));

            this.AddCommandName(DbCommandType.DisableConstraints, string.Format(disableConstraintsText, ParserName.Parse(TableDescription).Schema().Quoted().ToString()));
            this.AddCommandName(DbCommandType.EnableConstraints, string.Format(enableConstraintsText, ParserName.Parse(TableDescription).Schema().Quoted().ToString()));

            this.AddCommandName(DbCommandType.UpdateUntrackedRows, CreateUpdateUntrackedRowsCommand());
            this.AddCommandName(DbCommandType.SelectMetadata, CreateSelectMetadataCommand());
            this.AddCommandName(DbCommandType.UpdateMetadata, CreateUpdateMetadataCommand());
            this.AddCommandName(DbCommandType.SelectRow, CreateSelectRowCommand());
            this.AddCommandName(DbCommandType.Reset, CreateResetCommand());

        }

        private string CreateSelectMetadataCommand()
        {
            var stringBuilder = new StringBuilder();
            var pkeysSelect = new StringBuilder();
            var pkeysWhere = new StringBuilder();

            string and = string.Empty;
            string comma = string.Empty;
            foreach (var pkColumn in TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();
                pkeysSelect.Append(comma).Append("[side].").Append(columnName);

                pkeysWhere.Append(and).Append("[side].").Append(columnName).Append(" = @").Append(parameterName);

                and = " AND ";
                comma = ", ";
            }

            stringBuilder.Append("SELECT ").Append(pkeysSelect).AppendLine(", [side].[update_scope_id], [side].[timestamp_bigint] as [timestamp], [side].[sync_row_is_tombstone]");
            stringBuilder.Append("FROM ").Append(trackingName.Schema().Quoted().ToString()).AppendLine(" [side]");
            stringBuilder.Append("WHERE ").Append(pkeysWhere).AppendLine();

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
            foreach (var pkColumn in TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();

                pkeysForUpdate.Append(and).Append("[side].").Append(columnName).Append(" = @").Append(parameterName);

                pkeySelectForInsert.Append(comma).Append(columnName);
                pkeyISelectForInsert.Append(comma).Append("[i].").Append(columnName);
                pkeyAliasSelectForInsert.Append(comma).Append('@').Append(parameterName).Append(" as ").Append(columnName);
                pkeysLeftJoinForInsert.Append(and).Append("[side].").Append(columnName).Append(" = [i].").Append(columnName);
                pkeysIsNullForInsert.Append(and).Append("[side].").Append(columnName).Append(" IS NULL");
                and = " AND ";
                comma = ", ";
            }


            stringBuilder.AppendLine($"UPDATE [side] SET ");
            stringBuilder.AppendLine($" [update_scope_id] = @sync_scope_id, ");
            stringBuilder.AppendLine($" [sync_row_is_tombstone] = @sync_row_is_tombstone, ");
            stringBuilder.AppendLine($" [last_change_datetime] = GETUTCDATE() ");
            stringBuilder.Append("FROM ").Append(trackingName.Schema().Quoted()).AppendLine(" [side]");
            stringBuilder.Append($"WHERE ");
            stringBuilder.Append(pkeysForUpdate.ToString());
            stringBuilder.AppendLine($";");
            stringBuilder.AppendLine();

            stringBuilder.Append("INSERT INTO ").Append(trackingName.Schema().Quoted()).AppendLine(" (");
            stringBuilder.AppendLine(pkeySelectForInsert.ToString());
            stringBuilder.AppendLine(",[update_scope_id], [sync_row_is_tombstone],[last_change_datetime] )");
            stringBuilder.Append("SELECT ").Append(pkeyISelectForInsert).AppendLine(" ");
            stringBuilder.AppendLine($"   , i.sync_scope_id, i.sync_row_is_tombstone, i.UtcDate");
            stringBuilder.AppendLine("FROM (");
            stringBuilder.Append("  SELECT ").Append(pkeyAliasSelectForInsert).AppendLine();
            stringBuilder.AppendLine($"          ,@sync_scope_id as sync_scope_id, @sync_row_is_tombstone as sync_row_is_tombstone, GETUTCDATE() as UtcDate) as i");
            stringBuilder.Append("LEFT JOIN  ").Append(trackingName.Schema().Quoted()).Append(" [side] ON ").Append(pkeysLeftJoinForInsert).AppendLine(" ");
            stringBuilder.Append("WHERE ").Append(pkeysIsNullForInsert).AppendLine(";");


            return stringBuilder.ToString();
        }

        private string CreateUpdateUntrackedRowsCommand()
        {
            var stringBuilder = new StringBuilder();
            var str1 = new StringBuilder();
            var str2 = new StringBuilder();
            var str3 = new StringBuilder();
            var str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[side]", "[base]");

            stringBuilder.Append("INSERT INTO ").Append(trackingName.Schema().Quoted().ToString()).AppendLine(" (");


            var comma = "";
            foreach (var pkeyColumn in TableDescription.GetPrimaryKeysColumns())
            {
                var pkeyColumnName = ParserName.Parse(pkeyColumn).Quoted().ToString();

                str1.Append(comma).Append(pkeyColumnName);
                str2.Append(comma).Append("[base].").Append(pkeyColumnName);
                str3.Append(comma).Append("[side].").Append(pkeyColumnName);

                comma = ", ";
            }
            stringBuilder.Append(str1.ToString());
            stringBuilder.AppendLine($", [update_scope_id], [sync_row_is_tombstone], [last_change_datetime]");
            stringBuilder.AppendLine($")");
            stringBuilder.Append($"SELECT ");
            stringBuilder.Append(str2.ToString());
            stringBuilder.AppendLine($", NULL, 0, GetUtcDate()");
            stringBuilder.Append("FROM ").Append(tableName.Schema().Quoted().ToString()).AppendLine(" as [base] WHERE NOT EXISTS");
            stringBuilder.Append($"(SELECT ");
            stringBuilder.Append(str3.ToString());
            stringBuilder.Append(" FROM ").Append(trackingName.Schema().Quoted().ToString()).AppendLine(" as [side] ");
            stringBuilder.Append("WHERE ").Append(str4).AppendLine(")");

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

                stringBuilder1.Append(empty).Append("[side].").Append(columnName).Append(" = @").Append(parameterName);
                empty = " AND ";
            }
            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();

                var isPrimaryKey = this.TableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.Append("\t[side].").Append(columnName).AppendLine(", ");
                else
                    stringBuilder.Append("\t[base].").Append(columnName).AppendLine(", ");
            }
            stringBuilder.AppendLine($"\t[side].[sync_row_is_tombstone] as [sync_row_is_tombstone], ");
            stringBuilder.AppendLine($"\t[side].[update_scope_id] as [sync_update_scope_id]");

            stringBuilder.Append("FROM ").Append(tableName.Schema().Quoted()).AppendLine(" [base]");
            stringBuilder.Append("RIGHT JOIN ").Append(trackingName.Schema().Quoted()).AppendLine(" [side] ON");

            string str = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.Append(str).Append("[base].").Append(columnName).Append(" = [side].").Append(columnName);
                str = " AND ";
            }
            stringBuilder.AppendLine();
            stringBuilder.Append(string.Concat("WHERE ", stringBuilder1.ToString()));
            return stringBuilder.ToString();
        }

        private string CreateResetCommand()
        {
            var updTriggerName = GetTriggerCommandName(DbTriggerType.Update);
            var delTriggerName = GetTriggerCommandName(DbTriggerType.Delete);
            var insTriggerName = GetTriggerCommandName(DbTriggerType.Insert);

         

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"SET @sync_row_count = 0;");
            stringBuilder.AppendLine();

            stringBuilder.Append("DISABLE TRIGGER ").Append(updTriggerName).Append(" ON ").Append(tableName.Schema().Quoted()).AppendLine(";");
            stringBuilder.Append("DISABLE TRIGGER ").Append(insTriggerName).Append(" ON ").Append(tableName.Schema().Quoted()).AppendLine(";");
            stringBuilder.Append("DISABLE TRIGGER ").Append(delTriggerName).Append(" ON ").Append(tableName.Schema().Quoted()).AppendLine(";");

            stringBuilder.Append("DELETE FROM ").Append(tableName.Schema().Quoted()).AppendLine(";");
            stringBuilder.Append("DELETE FROM ").Append(trackingName.Schema().Quoted()).AppendLine(";");

            stringBuilder.Append("ENABLE TRIGGER ").Append(updTriggerName).Append(" ON ").Append(tableName.Schema().Quoted()).AppendLine(";");
            stringBuilder.Append("ENABLE TRIGGER ").Append(insTriggerName).Append(" ON ").Append(tableName.Schema().Quoted()).AppendLine(";");
            stringBuilder.Append("ENABLE TRIGGER ").Append(delTriggerName).Append(" ON ").Append(tableName.Schema().Quoted()).AppendLine(";");


            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("SET @sync_row_count = @@ROWCOUNT;"));

            return stringBuilder.ToString();
        }

    }
}
