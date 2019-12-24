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
        private ParserName tableName;
        private ParserName trackingName;
        private SyncTable tableDescription;
        private MySqlConnection connection;
        private MySqlTransaction transaction;
        private MySqlObjectNames mySqlObjectNames;

        public IEnumerable<SyncFilter> Filters { get; set; }



        public MySqlBuilderTrigger(SyncTable tableDescription, DbConnection connection, DbTransaction transaction = null)
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
            stringBuilder.AppendLine($"UPDATE {trackingName.Quoted().ToString()} ");
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
                    if (this.tableDescription.PrimaryKeys.Any(c => c.ToLowerInvariant() == filterColumn.ColumnName.ToLowerInvariant()))
                        continue;
                    var columnName = ParserName.Parse(filterColumn.ColumnName, "`").Quoted().ToString();

                    stringBuilder.AppendLine($"\t,{columnName} = `d`.{columnName}");

                }
                stringBuilder.AppendLine();
            }

            stringBuilder.Append($"WHERE ");
            stringBuilder.Append(MySqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, trackingName.Quoted().ToString(), "old"));
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

                    var delTriggerName = this.mySqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger).name;
                    StringBuilder createTrigger = new StringBuilder();
                    createTrigger.AppendLine($"CREATE TRIGGER {delTriggerName} AFTER DELETE ON {tableName.Quoted().ToString()} FOR EACH ROW ");
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

            var delTriggerName = string.Format(this.mySqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger).name, tableName.Unquoted().Normalized().ToString());
            StringBuilder createTrigger = new StringBuilder();
            createTrigger.AppendLine($"CREATE TRIGGER {delTriggerName} AFTER DELETE ON {tableName.Quoted().ToString()} FOR EACH ROW ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.DeleteTriggerBodyText());

            string str = $"Delete Trigger for table {tableName.Quoted().ToString()}";
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

            stringBuilder.AppendLine($"\tINSERT INTO {trackingName.Quoted().ToString()} (");

            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderArguments2 = new StringBuilder();
            StringBuilder stringPkAreNull = new StringBuilder();

            string argComma = string.Empty;
            string argAnd = string.Empty;
            foreach (var mutableColumn in this.tableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn, "`").Quoted().ToString();

                stringBuilderArguments.AppendLine($"\t\t{argComma}{columnName}");
                stringBuilderArguments2.AppendLine($"\t\t{argComma}new.{columnName}");
                stringPkAreNull.Append($"{argAnd}{trackingName.Quoted().ToString()}.{columnName} IS NULL");
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
            if (this.Filters != null && this.Filters.Count() > 0)
            {
                foreach (var filterColumn in this.Filters)
                {
                    if (this.tableDescription.PrimaryKeys.Any(c => c.ToLowerInvariant() == filterColumn.ColumnName.ToLowerInvariant()))
                        continue;

                    var columnName = ParserName.Parse(filterColumn.ColumnName, "`").Quoted().ToString();
                    filterColumnsString.AppendLine($"\t,{columnName}");
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

            if (Filters != null && Filters.Count() > 0)
                stringBuilder.AppendLine(filterColumnsString.ToString());

            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine("ON DUPLICATE KEY UPDATE");
            stringBuilder.AppendLine("\t`sync_row_is_tombstone` = 0, ");
            stringBuilder.AppendLine("\t`create_scope_id` = NULL, ");
            stringBuilder.AppendLine("\t`update_scope_id` = NULL, ");
            stringBuilder.AppendLine($"\t`create_timestamp` = {MySqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine($"\t`update_timestamp` = {MySqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine("\t`sync_row_is_tombstone` = 0, ");
            stringBuilder.AppendLine($"\t`timestamp` = {MySqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine("\t`last_change_datetime` = utc_timestamp()");

            if (Filters != null && Filters.Count() > 0)
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

                    var insTriggerName = string.Format(this.mySqlObjectNames.GetCommandName(DbCommandType.InsertTrigger).name, tableName.Unquoted().Normalized().ToString());

                    StringBuilder createTrigger = new StringBuilder();
                    createTrigger.AppendLine($"CREATE TRIGGER {insTriggerName} AFTER INSERT ON {tableName.Quoted().ToString()} FOR EACH ROW ");
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
            var insTriggerName = string.Format(this.mySqlObjectNames.GetCommandName(DbCommandType.InsertTrigger).name, tableName.Unquoted().Normalized().ToString());
            StringBuilder createTrigger = new StringBuilder();
            createTrigger.AppendLine($"CREATE TRIGGER {insTriggerName} AFTER INSERT ON {tableName.Quoted().ToString()} FOR EACH ROW ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            string str = $"Insert Trigger for table {tableName.Quoted().ToString()}";
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
            stringBuilder.AppendLine($"\tUPDATE {trackingName.Quoted().ToString()} ");
            stringBuilder.AppendLine("\tSET `update_scope_id` = NULL -- since the update if from local, it's a NULL");
            stringBuilder.AppendLine($"\t\t,`update_timestamp` = {MySqlObjectNames.TimestampValue}");
            stringBuilder.AppendLine($"\t\t,`timestamp` = {MySqlObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t\t,`last_change_datetime` = utc_timestamp()");

            if (this.Filters != null && Filters.Count() > 0)
            {
                foreach (var filterColumn in this.Filters)
                {
                    if (this.tableDescription.PrimaryKeys.Any(c => c.ToLowerInvariant() == filterColumn.ColumnName.ToLowerInvariant()))
                        continue;

                    var columnName = ParserName.Parse(filterColumn.ColumnName, "`").Quoted().ToString();

                    stringBuilder.AppendLine($"\t,{columnName} = `i`.{columnName}");
                }
                stringBuilder.AppendLine();
            }

            stringBuilder.Append($"\tWhere ");
            stringBuilder.Append(MySqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, trackingName.Quoted().ToString(), "new"));
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

                    var updTriggerName = string.Format(this.mySqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger).name, tableName.Unquoted().Normalized().ToString());
                    StringBuilder createTrigger = new StringBuilder();
                    createTrigger.AppendLine($"CREATE TRIGGER {updTriggerName} AFTER UPDATE ON {tableName.Quoted().ToString()} FOR EACH ROW ");
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
            var updTriggerName = string.Format(this.mySqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger).name, tableName.Unquoted().Normalized().ToString());
            StringBuilder createTrigger = new StringBuilder();
            createTrigger.AppendLine($"CREATE TRIGGER {updTriggerName} AFTER UPDATE ON {tableName.Quoted().ToString()} FOR EACH ROW ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.UpdateTriggerBodyText());

            string str = $"Update Trigger for table {tableName.Quoted().ToString()}";
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
            var updTriggerName = string.Format(this.mySqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger).name, tableName.Unquoted().Normalized().ToString());
            var delTriggerName = string.Format(this.mySqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger).name, tableName.Unquoted().Normalized().ToString());
            var insTriggerName = string.Format(this.mySqlObjectNames.GetCommandName(DbCommandType.InsertTrigger).name, tableName.Unquoted().Normalized().ToString());

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
            var triggerName = string.Format(this.mySqlObjectNames.GetCommandName(triggerType).name, tableName.Unquoted().Normalized().ToString());
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
            var commandName = this.mySqlObjectNames.GetCommandName(triggerType).name;
            var commandText = $"drop trigger if exists {commandName}";

            var str1 = $"Drop trigger {commandName} for table {tableName.Quoted().ToString()}";
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
