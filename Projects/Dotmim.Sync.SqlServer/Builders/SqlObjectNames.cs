using Dotmim.Sync.Core.Builders;
using System;
using System.Collections.Generic;
using System.Text;
using Dotmim.Sync.Data;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlObjectNames : DbObjectNames
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

        public SqlObjectNames(DmTable tableDescription) : base(tableDescription)
        {
            SetDefaultNames();
        }

        /// <summary>
        /// Set the default stored procedures names
        /// </summary>
        private void SetDefaultNames()
        {
            (var tableName, var trackingName) = SqlBuilder.GetParsers(this.TableDescription);

            this.AddName(DbObjectType.SelectChangesProcName, string.Format(selectChangesProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbObjectType.SelectRowProcName, string.Format(selectRowProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbObjectType.InsertProcName, string.Format(insertProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbObjectType.UpdateProcName, string.Format(updateProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbObjectType.DeleteProcName, string.Format(deleteProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbObjectType.InsertMetadataProcName, string.Format(insertMetadataProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbObjectType.UpdateMetadataProcName, string.Format(updateMetadataProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbObjectType.DeleteMetadataProcName, string.Format(deleteProcName, tableName.UnquotedStringWithUnderScore));

            this.AddName(DbObjectType.InsertTriggerName, string.Format(insertTriggerName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbObjectType.UpdateTriggerName, string.Format(updateTriggerName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbObjectType.DeleteTriggerName, string.Format(deleteTriggerName, tableName.UnquotedStringWithUnderScore));

            this.AddName(DbObjectType.BulkTableTypeName, string.Format(bulkTableTypeName, tableName.UnquotedStringWithUnderScore));

            this.AddName(DbObjectType.BulkInsertProcName, string.Format(bulkInsertProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbObjectType.BulkUpdateProcName, string.Format(bulkUpdateProcName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbObjectType.BulkDeleteProcName, string.Format(bulkDeleteProcName, tableName.UnquotedStringWithUnderScore));
        }

    }
}
