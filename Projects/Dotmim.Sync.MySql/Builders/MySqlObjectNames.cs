using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.MySql
{
    public class MySqlObjectNames
    {
        public const string TimestampValue = "ROUND(UNIX_TIMESTAMP(CURTIME(4)) * 10000)";

        internal const string insertTriggerName = "`{0}_insert_trigger`";
        internal const string updateTriggerName = "`{0}_update_trigger`";
        internal const string deleteTriggerName = "`{0}_delete_trigger`";

        internal const string selectChangesProcName = "`{0}_changes`";
        internal const string selectChangesProcNameWithFilters = "`{0}_{1}_changes`";

        internal const string initializeChangesProcName = "`{0}_initialize`";
        internal const string initializeChangesProcNameWithFilters = "`{0}_{1}_initialize`";

        internal const string selectRowProcName = "`{0}_selectrow`";

        internal const string insertProcName = "`{0}_insert`";
        internal const string updateProcName = "`{0}_update`";
        internal const string deleteProcName = "`{0}_delete`";

        internal const string resetProcName = "`{0}_reset`";

        internal const string insertMetadataProcName = "`{0}_insertmetadata`";
        internal const string updateMetadataProcName = "`{0}_updatemetadata`";
        internal const string deleteMetadataProcName = "`{0}_deletemetadata`";

        internal const string disableConstraintsText = "SET FOREIGN_KEY_CHECKS=0;";
        internal const string enableConstraintsText = "SET FOREIGN_KEY_CHECKS=1;";


        Dictionary<DbCommandType, (string name, bool isStoredProcedure)> names = new Dictionary<DbCommandType, (string name, bool isStoredProcedure)>();
        private ParserName tableName, trackingName;

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
                throw new NotSupportedException($"MySql provider does not support the command type {objectType.ToString()}");


            (var commandName, var isStoredProc) = names[objectType];

            // concat filter name
            if (filter != null)
                commandName = string.Format(commandName, filter.GetFilterName());

            return (commandName, isStoredProc);
        }

        public MySqlObjectNames(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
        {
            this.TableDescription = tableDescription;
            this.Setup = setup;
            this.tableName = tableName;
            this.trackingName = trackingName;

            SetDefaultNames();
        }

        /// <summary>
        /// Set the default stored procedures names
        /// </summary>
        private void SetDefaultNames()
        {
            var spPref = this.Setup.StoredProceduresPrefix != null ? this.Setup.StoredProceduresPrefix : "";
            var spSuf = this.Setup.StoredProceduresSuffix != null ? this.Setup.StoredProceduresSuffix : "";
            var trigPref = this.Setup.TriggersPrefix != null ? this.Setup.TriggersPrefix : "";
            var trigSuf = this.Setup.TriggersSuffix != null ? this.Setup.TriggersSuffix : "";

            this.AddName(DbCommandType.InsertTrigger, string.Format(insertTriggerName, $"{trigPref}{tableName.Unquoted().Normalized().ToString()}{trigSuf}"), true);
            this.AddName(DbCommandType.UpdateTrigger, string.Format(updateTriggerName, $"{trigPref}{tableName.Unquoted().Normalized().ToString()}{trigSuf}"), true);
            this.AddName(DbCommandType.DeleteTrigger, string.Format(deleteTriggerName, $"{trigPref}{tableName.Unquoted().Normalized().ToString()}{trigSuf}"), true);

            this.AddName(DbCommandType.SelectChanges, string.Format(selectChangesProcName, $"{spPref}{tableName.Unquoted().Normalized().ToString()}{spSuf}"), true);
            this.AddName(DbCommandType.SelectChangesWithFilters, string.Format(selectChangesProcNameWithFilters, $"{spPref}{tableName.Unquoted().Normalized().ToString()}{spSuf}", "{0}"), true);

            this.AddName(DbCommandType.SelectInitializedChanges, string.Format(initializeChangesProcName, $"{spPref}{tableName.Unquoted().Normalized().ToString()}{spSuf}"), true);
            this.AddName(DbCommandType.SelectInitializedChangesWithFilters, string.Format(initializeChangesProcNameWithFilters, $"{spPref}{tableName.Unquoted().Normalized().ToString()}{spSuf}", "{0}"), true);

            this.AddName(DbCommandType.SelectRow, string.Format(selectRowProcName, $"{spPref}{tableName.Unquoted().Normalized().ToString()}{spSuf}"), true);
            this.AddName(DbCommandType.UpdateRow, string.Format(updateProcName, $"{spPref}{tableName.Unquoted().Normalized().ToString()}{spSuf}"), true);
            this.AddName(DbCommandType.DeleteRow, string.Format(deleteProcName, $"{spPref}{tableName.Unquoted().Normalized().ToString()}{spSuf}"), true);
            this.AddName(DbCommandType.DeleteMetadata, string.Format(deleteMetadataProcName, $"{spPref}{tableName.Unquoted().Normalized().ToString()}{spSuf}"), true);
            this.AddName(DbCommandType.Reset, string.Format(resetProcName, $"{spPref}{tableName.Unquoted().Normalized().ToString()}{spSuf}"), true);

            this.AddName(DbCommandType.DisableConstraints, string.Format(disableConstraintsText, ParserName.Parse(TableDescription).Quoted().ToString()), false);
            this.AddName(DbCommandType.EnableConstraints, string.Format(enableConstraintsText, ParserName.Parse(TableDescription).Quoted().ToString()), false);

            this.AddName(DbCommandType.UpdateUntrackedRows, CreateUpdateUntrackedRowsCommand(), false);
        }

        private string CreateUpdateUntrackedRowsCommand()
        {
            var stringBuilder = new StringBuilder();
            var str1 = new StringBuilder();
            var str2 = new StringBuilder();
            var str3 = new StringBuilder();
            var str4 = MySqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.GetPrimaryKeysColumns(), "`side`", "`base`");

            stringBuilder.AppendLine($"INSERT INTO {trackingName.Schema().Quoted().ToString()} (");


            var comma = "";
            foreach (var pkeyColumn in TableDescription.GetPrimaryKeysColumns())
            {
                var pkeyColumnName = ParserName.Parse(pkeyColumn).Quoted().ToString();

                str1.Append($"{comma}{pkeyColumnName}");
                str2.Append($"{comma}`base`.{pkeyColumnName}");
                str3.Append($"{comma}`side`.{pkeyColumnName}");

                comma = ", ";
            }
            stringBuilder.Append(str1.ToString());
            stringBuilder.AppendLine($", `update_scope_id`, `sync_row_is_tombstone`, `timestamp`, `last_change_datetime`");
            stringBuilder.AppendLine($")");
            stringBuilder.Append($"SELECT ");
            stringBuilder.Append(str2.ToString());
            stringBuilder.AppendLine($", NULL, 0, {MySqlObjectNames.TimestampValue}, now()");
            stringBuilder.AppendLine($"FROM {tableName.Schema().Quoted().ToString()} as `base` WHERE NOT EXISTS");
            stringBuilder.Append($"(SELECT ");
            stringBuilder.Append(str3.ToString());
            stringBuilder.AppendLine($" FROM {trackingName.Schema().Quoted().ToString()} as `side` ");
            stringBuilder.AppendLine($"WHERE {str4})");

            var r = stringBuilder.ToString();

            return r;

        }


    }
}
