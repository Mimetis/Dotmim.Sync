using Dotmim.Sync.Builders;
using System;
using System.Text;
using Dotmim.Sync.Data;
using System.Data.Common;
using System.Linq;
using Dotmim.Sync.Log;
using System.Data;
using MySql.Data.MySqlClient;
using Dotmim.Sync.Filter;
using System.Diagnostics;
using System.Collections.Generic;

namespace Dotmim.Sync.MySql
{
    public class MySqlBuilderTrigger : IDbBuilderTriggerHelper
    {
        private ObjectNameParser tableName;
        private ObjectNameParser trackingName;
        private DmTable tableDescription;
        private MySqlConnection connection;
        private MySqlTransaction transaction;
        private MySqlObjectNames mySqlObjectNames;

        public IList<FilterClause2> Filters { get; set; }
        public MySqlBuilderTrigger(DmTable tableDescription, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as MySqlConnection;
            this.transaction = transaction as MySqlTransaction;
            this.tableDescription = tableDescription;
            (this.tableName, this.trackingName) = MySqlBuilder.GetParsers(this.tableDescription);
            this.mySqlObjectNames = new MySqlObjectNames(this.tableDescription);
        }

        private string DeleteTriggerBodyText()
        {
            List<DmColumn> addedColumns = new List<DmColumn>();

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"BEGIN");
            stringBuilder.AppendLine($"\tUPDATE {trackingName.FullQuotedString} ");
            stringBuilder.AppendLine($"\tSET");
            stringBuilder.AppendLine($"\t\t`sync_row_is_tombstone` = 1");
            stringBuilder.AppendLine($"\t\t,`update_scope_id` = NULL -- since the update if from local, it's a NULL");
            stringBuilder.AppendLine($"\t\t,`update_timestamp` = {MySqlObjectNames.TimestampValue}");
            stringBuilder.AppendLine($"\t\t,`timestamp` = {MySqlObjectNames.TimestampValue}");
            stringBuilder.AppendLine($"\t\t,`last_change_datetime` = utc_timestamp()");

            //-----------------------------------------------------------
            // Adding the foreign keys
            //-----------------------------------------------------------
            foreach (var pr in this.tableDescription.ChildRelations)
            {
                // get the parent columns to have the correct name of the column (if we have mulitple columns who is bind to same child table)
                // ie : AddressBillId and AddressInvoiceId
                foreach (var c in pr.ParentColumns)
                {
                    // dont add doublons
                    if (this.tableDescription.PrimaryKey.Columns.Any(col => col.ColumnName.ToLowerInvariant() == c.ColumnName.ToLowerInvariant()))
                        continue;

                    var quotedColumnName = new ObjectNameParser(c.ColumnName, "`", "`").FullQuotedString;
                    stringBuilder.AppendLine($"\t\t,{quotedColumnName} = old.{quotedColumnName}");

                    addedColumns.Add(c);
                }
            }

            // ---------------------------------------------------------------------
            // Add the filter columns if needed, and if not already added from Pkeys or Fkeys
            // ---------------------------------------------------------------------
            if (this.Filters != null && this.Filters.Count > 0)
            {
                foreach (var filter in this.Filters)
                {
                    // if column is null, we are in a table that need a relation before
                    if (string.IsNullOrEmpty(filter.FilterTable.ColumnName))
                        continue;

                    var columnFilter = this.tableDescription.Columns[filter.FilterTable.ColumnName];

                    if (columnFilter == null)
                        throw new InvalidExpressionException($"Column {filter.FilterTable.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                    if (addedColumns.Any(ac => ac.ColumnName.ToLowerInvariant() == columnFilter.ColumnName.ToLowerInvariant()))
                        continue;

                    var quotedColumnName = new ObjectNameParser(columnFilter.ColumnName, "`", "`").FullQuotedString;
                    stringBuilder.AppendLine($"\t\t,{quotedColumnName} = old.{quotedColumnName}");

                    addedColumns.Add(columnFilter);
                }
            }

            stringBuilder.Append($"WHERE ");
            stringBuilder.Append(MySqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, trackingName.FullQuotedString, "old"));
            stringBuilder.AppendLine(";");
            stringBuilder.AppendLine("END;");
            addedColumns.Clear();
            return stringBuilder.ToString();
        }
        public void CreateDeleteTrigger()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new MySqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var delTriggerName = this.mySqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger);
                    StringBuilder createTrigger = new StringBuilder();
                    createTrigger.AppendLine($"CREATE TRIGGER {delTriggerName} AFTER DELETE ON {tableName.FullQuotedString} FOR EACH ROW ");
                    createTrigger.AppendLine();
                    createTrigger.AppendLine(this.DeleteTriggerBodyText());

                    command.CommandText = createTrigger.ToString();
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateDeleteTrigger : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }
        }
        public string CreateDeleteTriggerScriptText()
        {

            var delTriggerName = string.Format(this.mySqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger), tableName.ObjectNameNormalized);
            StringBuilder createTrigger = new StringBuilder();
            createTrigger.AppendLine($"CREATE TRIGGER {delTriggerName} AFTER DELETE ON {tableName.FullQuotedString} FOR EACH ROW ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.DeleteTriggerBodyText());

            string str = $"Delete Trigger for table {tableName.FullQuotedString}";
            return MySqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }
        public void AlterDeleteTrigger()
        {


        }
        public string AlterDeleteTriggerScriptText()
        {
            return "";
        }

        /// <summary>
        /// TODO : Check if row was deleted before, to just make an update !!!!
        /// </summary>
        /// <returns></returns>
        private string InsertTriggerBodyText()
        {
            List<DmColumn> addedColumns = new List<DmColumn>();

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- If row was deleted before, it already exists, so just make an update");
            stringBuilder.AppendLine("BEGIN");

            stringBuilder.AppendLine($"\tINSERT INTO {trackingName.FullQuotedString} (");

            StringBuilder columnsString = new StringBuilder();
            StringBuilder valuesString = new StringBuilder();

            string argComma = string.Empty;
            foreach (var mutableColumn in this.tableDescription.PrimaryKey.Columns.Where(c => !c.IsReadOnly))
            {
                ObjectNameParser columnName = new ObjectNameParser(mutableColumn.ColumnName, "`", "`");
                columnsString.AppendLine($"\t\t{argComma}{columnName.FullQuotedString}");
                valuesString.AppendLine($"\t\t{argComma}new.{columnName.FullQuotedString}");

                argComma = ",";
            }

            stringBuilder.Append(columnsString.ToString());
            stringBuilder.AppendLine("\t\t,`create_scope_id`");
            stringBuilder.AppendLine("\t\t,`create_timestamp`");
            stringBuilder.AppendLine("\t\t,`update_scope_id`");
            stringBuilder.AppendLine("\t\t,`update_timestamp`");
            stringBuilder.AppendLine("\t\t,`timestamp`");
            stringBuilder.AppendLine("\t\t,`sync_row_is_tombstone`");
            stringBuilder.AppendLine("\t\t,`last_change_datetime`");



            //-----------------------------------------------------------
            // Adding the foreign keys
            //-----------------------------------------------------------

            // supports all ",`CustomerId`"
            StringBuilder columnsfiltersString = new StringBuilder();
            // supports all ",new.`CustomerId`"
            StringBuilder valuesfiltersString = new StringBuilder();
            // supports all ",`CustomerId` = new.`CustomerId`
            StringBuilder bothfiltersString = new StringBuilder();

            foreach (var pkey in this.tableDescription.PrimaryKey.Columns)
                addedColumns.Add(pkey);

            foreach (var pr in this.tableDescription.ChildRelations)
            {
                // get the parent columns to have the correct name of the column (if we have mulitple columns who is bind to same child table)
                // ie : AddressBillId and AddressInvoiceId
                foreach (var c in pr.ParentColumns)
                {
                    // dont add doublons
                    if (addedColumns.Any(col => col.ColumnName.ToLowerInvariant() == c.ColumnName.ToLowerInvariant()))
                        continue;

                    var quotedColumnName = new ObjectNameParser(c.ColumnName, "`", "`").FullQuotedString;
                    columnsfiltersString.AppendLine($"\t\t,{quotedColumnName}");
                    valuesfiltersString.AppendLine($"\t\t,new.{quotedColumnName}");
                    bothfiltersString.AppendLine($"\t\t,{quotedColumnName} = new.{quotedColumnName}");
                    addedColumns.Add(c);
                }
            }

            // ---------------------------------------------------------------------
            // Add the filter columns if needed, and if not already added from Pkeys or Fkeys
            // ---------------------------------------------------------------------
            if (this.Filters != null && this.Filters.Count > 0)
            {
                foreach (var filter in this.Filters)
                {
                    // if column is null, we are in a table that need a relation before
                    if (string.IsNullOrEmpty(filter.FilterTable.ColumnName))
                        continue;

                    var columnFilter = this.tableDescription.Columns[filter.FilterTable.ColumnName];

                    if (columnFilter == null)
                        throw new InvalidExpressionException($"Column {filter.FilterTable.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                    if (addedColumns.Any(ac => ac.ColumnName.ToLowerInvariant() == columnFilter.ColumnName.ToLowerInvariant()))
                        continue;

                    var quotedColumnName = new ObjectNameParser(columnFilter.ColumnName, "`", "`").FullQuotedString;
                    columnsfiltersString.AppendLine($"\t\t,{quotedColumnName}");
                    valuesfiltersString.AppendLine($"\t\t,new.{quotedColumnName}");
                    bothfiltersString.AppendLine($"\t\t,{quotedColumnName} = new.{quotedColumnName}");

                    addedColumns.Add(columnFilter);
                }
            }

            addedColumns.Clear();

            stringBuilder.AppendLine(columnsfiltersString.ToString());
            stringBuilder.AppendLine("\t) ");
            stringBuilder.AppendLine("\tVALUES (");
            stringBuilder.Append(valuesString.ToString());
            stringBuilder.AppendLine("\t\t,NULL");
            stringBuilder.AppendLine($"\t\t,{MySqlObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t\t,NULL");
            stringBuilder.AppendLine("\t\t,0");
            stringBuilder.AppendLine($"\t\t,{MySqlObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t\t,0");
            stringBuilder.AppendLine("\t\t,utc_timestamp()");
            stringBuilder.AppendLine(valuesfiltersString.ToString());

            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine($"ON DUPLICATE KEY UPDATE");
            stringBuilder.AppendLine($"\t\t`sync_row_is_tombstone` = 0 ");
            stringBuilder.AppendLine($"\t\t,`create_scope_id` = NULL ");
            stringBuilder.AppendLine($"\t\t,`update_scope_id` = NULL ");
            stringBuilder.AppendLine($"\t\t,`create_timestamp` = {MySqlObjectNames.TimestampValue} ");
            stringBuilder.AppendLine($"\t\t,`update_timestamp` = NULL ");
            stringBuilder.AppendLine($"\t\t,`sync_row_is_tombstone` = 0 ");
            stringBuilder.AppendLine($"\t\t,`timestamp` = {MySqlObjectNames.TimestampValue} ");
            stringBuilder.AppendLine($"\t\t,`last_change_datetime` = utc_timestamp()");
            stringBuilder.AppendLine(bothfiltersString.ToString());
            stringBuilder.AppendLine($"\t\t;");

            stringBuilder.AppendLine("END;");
            return stringBuilder.ToString();
        }
        public void CreateInsertTrigger()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new MySqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var insTriggerName = string.Format(this.mySqlObjectNames.GetCommandName(DbCommandType.InsertTrigger), tableName.ObjectNameNormalized);

                    StringBuilder createTrigger = new StringBuilder();
                    createTrigger.AppendLine($"CREATE TRIGGER {insTriggerName} AFTER INSERT ON {tableName.FullQuotedString} FOR EACH ROW ");
                    createTrigger.AppendLine();
                    createTrigger.AppendLine(this.InsertTriggerBodyText());

                    command.CommandText = createTrigger.ToString();
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateDeleteTrigger : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }
        }
        public string CreateInsertTriggerScriptText()
        {
            var insTriggerName = string.Format(this.mySqlObjectNames.GetCommandName(DbCommandType.InsertTrigger), tableName.ObjectNameNormalized);
            StringBuilder createTrigger = new StringBuilder();
            createTrigger.AppendLine($"CREATE TRIGGER {insTriggerName} AFTER INSERT ON {tableName.FullQuotedString} FOR EACH ROW ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            string str = $"Insert Trigger for table {tableName.FullQuotedString}";
            return MySqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);

        }
        public void AlterInsertTrigger()
        {

        }
        public string AlterInsertTriggerScriptText()
        {
            return "";
        }


        private string UpdateTriggerBodyText()
        {
            var addedColumns = new List<DmColumn>();

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"Begin ");
            stringBuilder.AppendLine($"\tUPDATE {trackingName.FullQuotedString} ");
            stringBuilder.AppendLine($"\tSET");
            stringBuilder.AppendLine($"\t\t`update_scope_id` = NULL -- since the update if from local, it's a NULL");
            stringBuilder.AppendLine($"\t\t,`update_timestamp` = {MySqlObjectNames.TimestampValue}");
            stringBuilder.AppendLine($"\t\t,`timestamp` = {MySqlObjectNames.TimestampValue}");
            stringBuilder.AppendLine($"\t\t,`last_change_datetime` = utc_timestamp()");

            //-----------------------------------------------------------
            // Adding the foreign keys
            //-----------------------------------------------------------
            foreach (var pkey in this.tableDescription.PrimaryKey.Columns)
                addedColumns.Add(pkey);

            foreach (var pr in this.tableDescription.ChildRelations)
            {
                // get the parent columns to have the correct name of the column (if we have mulitple columns who is bind to same child table)
                // ie : AddressBillId and AddressInvoiceId
                foreach (var c in pr.ParentColumns)
                {
                    // dont add doublons
                    if (this.tableDescription.PrimaryKey.Columns.Any(col => col.ColumnName.ToLowerInvariant() == c.ColumnName.ToLowerInvariant()))
                        continue;

                    var quotedColumnName = new ObjectNameParser(c.ColumnName, "`", "`").FullQuotedString;
                    stringBuilder.AppendLine($"\t\t,{quotedColumnName} = new.{quotedColumnName}"); ;

                    addedColumns.Add(c);
                }
            }

            // ---------------------------------------------------------------------
            // Add the filter columns if needed, and if not already added from Pkeys or Fkeys
            // ---------------------------------------------------------------------
            if (this.Filters != null && this.Filters.Count > 0)
            {
                foreach (var filter in this.Filters)
                {
                    // if column is null, we are in a table that need a relation before
                    if (string.IsNullOrEmpty(filter.FilterTable.ColumnName))
                        continue;

                    var columnFilter = this.tableDescription.Columns[filter.FilterTable.ColumnName];

                    if (columnFilter == null)
                        throw new InvalidExpressionException($"Column {filter.FilterTable.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                    if (addedColumns.Any(ac => ac.ColumnName.ToLowerInvariant() == columnFilter.ColumnName.ToLowerInvariant()))
                        continue;

                    var quotedColumnName = new ObjectNameParser(columnFilter.ColumnName, "`", "`").FullQuotedString;
                    stringBuilder.AppendLine($"\t\t,{quotedColumnName} = new.{quotedColumnName}"); ;

                    addedColumns.Add(columnFilter);
                }
            }

            stringBuilder.Append($"\tWhere ");
            stringBuilder.Append(MySqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, trackingName.FullQuotedString, "new"));
            stringBuilder.AppendLine($"; ");
            stringBuilder.AppendLine($"End; ");
            return stringBuilder.ToString();
        }
        public void CreateUpdateTrigger()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new MySqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var updTriggerName = string.Format(this.mySqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger), tableName.ObjectNameNormalized);
                    StringBuilder createTrigger = new StringBuilder();
                    createTrigger.AppendLine($"CREATE TRIGGER {updTriggerName} AFTER UPDATE ON {tableName.FullQuotedString} FOR EACH ROW ");
                    createTrigger.AppendLine();
                    createTrigger.AppendLine(this.UpdateTriggerBodyText());

                    command.CommandText = createTrigger.ToString();
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateDeleteTrigger : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }
        }
        public string CreateUpdateTriggerScriptText()
        {
            var updTriggerName = string.Format(this.mySqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger), tableName.ObjectNameNormalized);
            StringBuilder createTrigger = new StringBuilder();
            createTrigger.AppendLine($"CREATE TRIGGER {updTriggerName} AFTER UPDATE ON {tableName.FullQuotedString} FOR EACH ROW ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.UpdateTriggerBodyText());

            string str = $"Update Trigger for table {tableName.FullQuotedString}";
            return MySqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }
        public void AlterUpdateTrigger()
        {
            return;
        }
        public string AlterUpdateTriggerScriptText()
        {
            return string.Empty;
        }
        public bool NeedToCreateTrigger(DbTriggerType type)
        {
            var updTriggerName = string.Format(this.mySqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger), tableName.ObjectNameNormalized);
            var delTriggerName = string.Format(this.mySqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger), tableName.ObjectNameNormalized);
            var insTriggerName = string.Format(this.mySqlObjectNames.GetCommandName(DbCommandType.InsertTrigger), tableName.ObjectNameNormalized);

            string triggerName = string.Empty;
            switch (type)
            {
                case DbTriggerType.Insert:
                    {
                        triggerName = insTriggerName;
                        break;
                    }
                case DbTriggerType.Update:
                    {
                        triggerName = updTriggerName;
                        break;
                    }
                case DbTriggerType.Delete:
                    {
                        triggerName = delTriggerName;
                        break;
                    }
            }

            return !MySqlManagementUtils.TriggerExists(connection, transaction, triggerName);

        }

        public void DropTrigger(DbCommandType triggerType)
        {
            var triggerName = string.Format(this.mySqlObjectNames.GetCommandName(triggerType), tableName.ObjectNameNormalized);
            var commandText = $"drop trigger if exists {triggerName}";

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                using (var command = new MySqlCommand(commandText, connection))
                {
                    if (transaction != null)
                        command.Transaction = transaction;

                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during DropTriggerCommand : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }


        public void DropInsertTrigger()
        {
            DropTrigger(DbCommandType.InsertTrigger);
        }

        public void DropUpdateTrigger()
        {
            DropTrigger(DbCommandType.UpdateTrigger);
        }

        public void DropDeleteTrigger()
        {
            DropTrigger(DbCommandType.DeleteTrigger);
        }

        private string DropTriggerText(DbCommandType triggerType)
        {
            var commandName = this.mySqlObjectNames.GetCommandName(triggerType);
            var commandText = $"drop trigger if exists {commandName}";

            var str1 = $"Drop trigger {commandName} for table {tableName.FullQuotedString}";
            return MySqlBuilder.WrapScriptTextWithComments(commandText, str1);

        }

        public string DropInsertTriggerScriptText()
        {
            return DropTriggerText(DbCommandType.InsertTrigger);
        }

        public string DropUpdateTriggerScriptText()
        {
            return DropTriggerText(DbCommandType.UpdateTrigger);
        }

        public string DropDeleteTriggerScriptText()
        {
            return DropTriggerText(DbCommandType.DeleteTrigger);
        }
    }
}
