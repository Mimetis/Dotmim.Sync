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

        public ICollection<FilterClause> Filters { get; set; }



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
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("BEGIN");
            stringBuilder.AppendLine($"UPDATE {trackingName.FullQuotedString} ");
            stringBuilder.AppendLine("SET `sync_row_is_tombstone` = 1");
            stringBuilder.AppendLine("\t,`update_scope_id` = NULL -- since the update if from local, it's a NULL");
            stringBuilder.AppendLine($"\t,`update_timestamp` = {MySqlObjectNames.TimestampValue}");
            stringBuilder.AppendLine($"\t,`timestamp` = {MySqlObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t,`last_change_datetime` = utc_timestamp()");

            // Filter columns
            if (this.Filters != null)
            {

                foreach (var filterColumn in this.Filters)
                {
                    if (this.tableDescription.PrimaryKey.Columns.Any(c => c.ColumnName.ToLowerInvariant() == filterColumn.ColumnName.ToLowerInvariant()))
                        continue;

                    ObjectNameParser columnName = new ObjectNameParser(filterColumn.ColumnName.ToLowerInvariant(), "`", "`");

                    stringBuilder.AppendLine($"\t,{columnName.FullQuotedString} = `d`.{columnName.FullQuotedString}");

                }
                stringBuilder.AppendLine();
            }

            stringBuilder.Append($"WHERE ");
            stringBuilder.Append(MySqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, trackingName.FullQuotedString, "old"));
            stringBuilder.AppendLine(";");
            stringBuilder.AppendLine("END;");
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
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- If row was deleted before, it already exists, so just make an update");
            stringBuilder.AppendLine("BEGIN");

            stringBuilder.AppendLine($"\tINSERT INTO {trackingName.FullQuotedString} (");

            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderArguments2 = new StringBuilder();
            StringBuilder stringPkAreNull = new StringBuilder();

            string argComma = string.Empty;
            string argAnd = string.Empty;
            foreach (var mutableColumn in this.tableDescription.PrimaryKey.Columns.Where(c => !c.IsReadOnly))
            {
                ObjectNameParser columnName = new ObjectNameParser(mutableColumn.ColumnName.ToLowerInvariant(), "`", "`");
                stringBuilderArguments.AppendLine($"\t\t{argComma}{columnName.FullQuotedString}");
                stringBuilderArguments2.AppendLine($"\t\t{argComma}new.{columnName.FullQuotedString}");
                stringPkAreNull.Append($"{argAnd}{trackingName.FullQuotedString}.{columnName.FullQuotedString} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine("\t\t,`create_scope_id`");
            stringBuilder.AppendLine("\t\t,`create_timestamp`");
            stringBuilder.AppendLine("\t\t,`update_scope_id`");
            stringBuilder.AppendLine("\t\t,`update_timestamp`");
            stringBuilder.AppendLine("\t\t,`timestamp`");
            stringBuilder.AppendLine("\t\t,`sync_row_is_tombstone`");
            stringBuilder.AppendLine("\t\t,`last_change_datetime`");

            StringBuilder filterColumnsString = new StringBuilder();

            // Filter columns
            if (this.Filters != null && this.Filters.Count > 0)
            {
                foreach (var filterColumn in this.Filters)
                {
                    if (this.tableDescription.PrimaryKey.Columns.Any(c => c.ColumnName.ToLowerInvariant() == filterColumn.ColumnName.ToLowerInvariant()))
                        continue;

                    ObjectNameParser columnName = new ObjectNameParser(filterColumn.ColumnName.ToLowerInvariant(), "`", "`");
                    filterColumnsString.AppendLine($"\t,{columnName.FullQuotedString}");
                }
                stringBuilder.AppendLine(filterColumnsString.ToString());
            }

            stringBuilder.AppendLine("\t) ");
            stringBuilder.AppendLine("\tVALUES (");
            stringBuilder.Append(stringBuilderArguments2.ToString());
            stringBuilder.AppendLine("\t\t,NULL");
            stringBuilder.AppendLine($"\t\t,{MySqlObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t\t,NULL");
            stringBuilder.AppendLine("\t\t,0");
            stringBuilder.AppendLine($"\t\t,{MySqlObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t\t,0");
            stringBuilder.AppendLine("\t\t,utc_timestamp()");

            if (Filters != null && Filters.Count > 0)
                stringBuilder.AppendLine(filterColumnsString.ToString());

            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine("ON DUPLICATE KEY UPDATE");
            stringBuilder.AppendLine("\t`sync_row_is_tombstone` = 0, ");
            stringBuilder.AppendLine("\t`create_scope_id` = NULL, ");
            stringBuilder.AppendLine("\t`update_scope_id` = NULL, ");
            stringBuilder.AppendLine($"\t`create_timestamp` = {MySqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine("\t`update_timestamp` = NULL, ");
            stringBuilder.AppendLine("\t`sync_row_is_tombstone` = 0, ");
            stringBuilder.AppendLine($"\t`timestamp` = {MySqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine("\t`last_change_datetime` = utc_timestamp()");

            if (Filters != null && Filters.Count > 0)
                stringBuilder.AppendLine(filterColumnsString.ToString());

            stringBuilder.Append(";");
            stringBuilder.AppendLine("END");
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
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"Begin ");
            stringBuilder.AppendLine($"\tUPDATE {trackingName.FullQuotedString} ");
            stringBuilder.AppendLine("\tSET `update_scope_id` = NULL -- since the update if from local, it's a NULL");
            stringBuilder.AppendLine($"\t\t,`update_timestamp` = {MySqlObjectNames.TimestampValue}");
            stringBuilder.AppendLine($"\t\t,`timestamp` = {MySqlObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t\t,`last_change_datetime` = utc_timestamp()");

            if (this.Filters != null && Filters.Count > 0)
            {
                foreach (var filterColumn in this.Filters)
                {
                    if (this.tableDescription.PrimaryKey.Columns.Any(c => c.ColumnName.ToLowerInvariant() == filterColumn.ColumnName.ToLowerInvariant()))
                        continue;

                    ObjectNameParser columnName = new ObjectNameParser(filterColumn.ColumnName.ToLowerInvariant(), "`", "`");
                    stringBuilder.AppendLine($"\t,{columnName.FullQuotedString} = `i`.{columnName.FullQuotedString}");
                }
                stringBuilder.AppendLine();
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
