using Dotmim.Sync.Core.Builders;
using System;
using System.Collections.Generic;
using System.Text;
using Dotmim.Sync.Data;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlObjectNames
    {
        internal const string insertTriggerName = "[{0}_insert_trigger]";
        internal const string updateTriggerName = "[{0}_update_trigger]";
        internal const string deleteTriggerName = "[{0}_delete_trigger]";

        internal const string selectChangesProcName = "[{0}_selectchanges]";
        internal const string selectRowProcName = "[{0}_selectrow]";

        internal const string insertProcName = "[{0}_insert]";
        internal const string updateProcName = "[{0}_update]";
        internal const string deleteProcName = "[{0}_delete]";

        internal const string insertMetadataProcName = "[{0}_insertmetadata]";
        internal const string updateMetadataProcName = "[{0}_updatemetadata]";
        internal const string deleteMetadataProcName = "[{0}_deletemetadata]";

        internal const string bulkTableTypeName = "[{0}_BulkType]";
        internal const string bulkInsertProcName = "[{0}_bulkinsert]";
        internal const string bulkUpdateProcName = "[{0}_bulkupdate]";
        internal const string bulkDeleteProcName = "[{0}_bulkdelete]";


        Dictionary<DbCommandType, String> names = new Dictionary<DbCommandType, string>();
        public DmTable TableDescription { get; }

    
        public void AddName(DbCommandType objectType, string name)
        {
            if (names.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            names.Add(objectType, name);
        }
        public string GetCommandName(DbCommandType objectType)
        {
            if (!names.ContainsKey(objectType))
                throw new Exception("Yous should provide a value for all DbCommandName");

            return names[objectType];
        }

        public SqlObjectNames(DmTable tableDescription) 
        {
            this.TableDescription = tableDescription;
            SetDefaultNames();
        }

        /// <summary>
        /// Set the default stored procedures names
        /// </summary>
        private void SetDefaultNames()
        {
            (var tableName, var trackingName) = SqlBuilder.GetParsers(this.TableDescription);

            this.AddName(DbCommandType.SelectChanges, string.Format(selectChangesProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.SelectRow, string.Format(selectRowProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.InsertRow, string.Format(insertProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.UpdateRow, string.Format(updateProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.DeleteRow, string.Format(deleteProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.InsertMetadata, string.Format(insertMetadataProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.UpdateMetadata, string.Format(updateMetadataProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.DeleteMetadata, string.Format(deleteMetadataProcName, tableName.UnquotedStringWithUnderScore));

            this.AddName(DbCommandType.InsertTrigger, string.Format(insertTriggerName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.UpdateTrigger, string.Format(updateTriggerName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.DeleteTrigger, string.Format(deleteTriggerName, tableName.UnquotedStringWithUnderScore));

            this.AddName(DbCommandType.BulkTableType, string.Format(bulkTableTypeName, tableName.UnquotedStringWithUnderScore));

            this.AddName(DbCommandType.BulkInsertRows, string.Format(bulkInsertProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.BulkUpdateRows, string.Format(bulkUpdateProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.BulkDeleteRows, string.Format(bulkDeleteProcName, tableName.UnquotedStringWithUnderScore));
        }

    }
}
