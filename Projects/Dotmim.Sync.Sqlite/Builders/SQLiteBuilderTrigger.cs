using Dotmim.Sync.Builders;
using System;
using System.Text;

using System.Data.Common;
using System.Linq;
using Dotmim.Sync.Log;
using System.Data;
using Microsoft.Data.Sqlite;

using System.Diagnostics;
using System.Collections.Generic;

namespace Dotmim.Sync.Sqlite
{
    public class SqliteBuilderTrigger : IDbBuilderTriggerHelper
    {
        private ParserName tableName;
        private ParserName trackingName;
        private SyncTable tableDescription;
        private SqliteConnection connection;
        private SqliteTransaction transaction;
        private SqliteObjectNames sqliteObjectNames;

        public SyncFilter Filter { get; set; }



        public SqliteBuilderTrigger(SyncTable tableDescription, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as SqliteConnection;
            this.transaction = transaction as SqliteTransaction;
            this.tableDescription = tableDescription;
            (this.tableName, this.trackingName) = SqliteTableBuilder.GetParsers(this.tableDescription);
            this.sqliteObjectNames = new SqliteObjectNames(this.tableDescription);
        }

        private string DeleteTriggerBodyText()
        {
            var stringBuilder = new StringBuilder();
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();
            string argComma = string.Empty;
            string argAnd = string.Empty;

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("BEGIN");

            stringBuilder.AppendLine($"\tINSERT OR REPLACE INTO {trackingName.Quoted().ToString()} (");
            foreach (var mutableColumn in this.tableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();

                stringBuilderArguments.AppendLine($"\t\t{argComma}{columnName}");
                stringBuilderArguments2.AppendLine($"\t\t{argComma}old.{columnName}");
                stringPkAreNull.Append($"{argAnd}{trackingName.Quoted().ToString()}.{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine("\t\t,[update_scope_id]");
            stringBuilder.AppendLine("\t\t,[timestamp]");
            stringBuilder.AppendLine("\t\t,[sync_row_is_tombstone]");
            stringBuilder.AppendLine("\t\t,[last_change_datetime]");

            stringBuilder.AppendLine("\t) ");
            stringBuilder.AppendLine("\tVALUES (");
            stringBuilder.Append(stringBuilderArguments2.ToString());
            stringBuilder.AppendLine("\t\t,NULL");
            stringBuilder.AppendLine($"\t\t,{SqliteObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t\t,1");
            stringBuilder.AppendLine("\t\t,datetime('now')");
            stringBuilder.AppendLine("\t);");
            stringBuilder.AppendLine("END;");
            return stringBuilder.ToString();
        }
        public void CreateDeleteTrigger()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqliteCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var delTriggerName = this.sqliteObjectNames.GetCommandName(DbCommandType.DeleteTrigger);
                    StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER IF NOT EXISTS {delTriggerName} AFTER DELETE ON {tableName.Quoted().ToString()} ");
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

            var delTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.DeleteTrigger), tableName.Unquoted().ToString());
            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER IF NOT EXISTS {delTriggerName} AFTER DELETE ON {tableName.Quoted().ToString()} ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.DeleteTriggerBodyText());

            string str = $"Delete Trigger for table {tableName.Quoted().ToString()}";
            return SqliteTableBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }
        public void AlterDeleteTrigger()
        {


        }
        public string AlterDeleteTriggerScriptText()
        {
            return "";
        }

        private string InsertTriggerBodyText()
        {
            var stringBuilder = new StringBuilder();
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();
            string argComma = string.Empty;
            string argAnd = string.Empty;

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("BEGIN");
            stringBuilder.AppendLine("-- If row was deleted before, it already exists, so just make an update");

            stringBuilder.AppendLine($"\tINSERT OR REPLACE INTO {trackingName.Quoted().ToString()} (");
            foreach (var mutableColumn in this.tableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();

                stringBuilderArguments.AppendLine($"\t\t{argComma}{columnName}");
                stringBuilderArguments2.AppendLine($"\t\t{argComma}new.{columnName}");
                stringPkAreNull.Append($"{argAnd}{trackingName.Quoted().ToString()}.{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine("\t\t,[update_scope_id]");
            stringBuilder.AppendLine("\t\t,[timestamp]");
            stringBuilder.AppendLine("\t\t,[sync_row_is_tombstone]");
            stringBuilder.AppendLine("\t\t,[last_change_datetime]");

            stringBuilder.AppendLine("\t) ");
            stringBuilder.AppendLine("\tVALUES (");
            stringBuilder.Append(stringBuilderArguments2.ToString());
            stringBuilder.AppendLine("\t\t,NULL");
            stringBuilder.AppendLine($"\t\t,{SqliteObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t\t,0");
            stringBuilder.AppendLine("\t\t,datetime('now')");
            stringBuilder.AppendLine("\t);");
            stringBuilder.AppendLine("END;");
            return stringBuilder.ToString();
        }
        public void CreateInsertTrigger()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqliteCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var insTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.InsertTrigger), tableName.Unquoted().ToString());

                    StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER IF NOT EXISTS {insTriggerName} AFTER INSERT ON {tableName.Quoted().ToString()} ");
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
            var insTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.InsertTrigger), tableName.Unquoted().ToString());
            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER IF NOT EXISTS {insTriggerName} AFTER INSERT ON {tableName.Quoted().ToString()} ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            string str = $"Insert Trigger for table {tableName.Quoted().ToString()}";
            return SqliteTableBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);

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
            stringBuilder.AppendLine("\tSET [update_scope_id] = NULL -- scope id is always NULL when update is made locally");
            stringBuilder.AppendLine($"\t\t,[timestamp] = {SqliteObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t\t,[last_change_datetime] = datetime('now')");

            stringBuilder.Append($"\tWhere ");
            stringBuilder.Append(SqliteManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, trackingName.Quoted().ToString(), "new"));


            if (this.tableDescription.GetMutableColumns().Count() > 0)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine("\t AND (");
                string or = "    ";
                foreach (var column in this.tableDescription.GetMutableColumns())
                {
                    var quotedColumn = ParserName.Parse(column).Quoted().ToString();

                    stringBuilder.Append("\t");
                    stringBuilder.Append(or);
                    stringBuilder.Append("IFNULL(");
                    stringBuilder.Append("NULLIF(");
                    stringBuilder.Append("[old].");
                    stringBuilder.Append(quotedColumn);
                    stringBuilder.Append(", ");
                    stringBuilder.Append("[new].");
                    stringBuilder.Append(quotedColumn);
                    stringBuilder.Append(")");
                    stringBuilder.Append(", ");
                    stringBuilder.Append("NULLIF(");
                    stringBuilder.Append("[new].");
                    stringBuilder.Append(quotedColumn);
                    stringBuilder.Append(", ");
                    stringBuilder.Append("[old].");
                    stringBuilder.Append(quotedColumn);
                    stringBuilder.Append(")");
                    stringBuilder.AppendLine(") IS NOT NULL");

                    or = " OR ";
                }
                stringBuilder.AppendLine("\t ) ");
            }

            stringBuilder.AppendLine($"; ");


            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();
            string argComma = string.Empty;
            string argAnd = string.Empty;

            stringBuilder.AppendLine($"\tINSERT OR IGNORE INTO {trackingName.Quoted().ToString()} (");
            foreach (var mutableColumn in this.tableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();

                stringBuilderArguments.AppendLine($"\t\t{argComma}{columnName}");
                stringBuilderArguments2.AppendLine($"\t\t{argComma}new.{columnName}");
                stringPkAreNull.Append($"{argAnd}{trackingName.Quoted().ToString()}.{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine("\t\t,[update_scope_id]");
            stringBuilder.AppendLine("\t\t,[timestamp]");
            stringBuilder.AppendLine("\t\t,[sync_row_is_tombstone]");
            stringBuilder.AppendLine("\t\t,[last_change_datetime]");

            stringBuilder.AppendLine("\t) ");
            stringBuilder.AppendLine("\tVALUES (");
            stringBuilder.Append(stringBuilderArguments2.ToString());
            stringBuilder.AppendLine("\t\t,NULL");
            stringBuilder.AppendLine($"\t\t,{SqliteObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t\t,0");
            stringBuilder.AppendLine("\t\t,datetime('now')");
            stringBuilder.AppendLine("\t);");

            stringBuilder.AppendLine($"End; ");
            return stringBuilder.ToString();
        }
        public void CreateUpdateTrigger()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqliteCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var updTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.UpdateTrigger), tableName.Unquoted().ToString());
                    StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER IF NOT EXISTS {updTriggerName} AFTER UPDATE ON {tableName.Quoted().ToString()} ");
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
            var updTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.UpdateTrigger), tableName.Unquoted().ToString());
            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER IF NOT EXISTS {updTriggerName} AFTER UPDATE ON {tableName.Quoted().ToString()} ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.UpdateTriggerBodyText());

            string str = $"Update Trigger for table {tableName.Quoted().ToString()}";
            return SqliteTableBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
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
            var updTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.UpdateTrigger), tableName.Unquoted().ToString());
            var delTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.DeleteTrigger), tableName.Unquoted().ToString());
            var insTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.InsertTrigger), tableName.Unquoted().ToString());

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

            return !SqliteManagementUtils.TriggerExists(connection, transaction, triggerName);


        }


        private void DropTrigger(DbCommandType triggerType)
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqliteCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var triggerName = string.Format(this.sqliteObjectNames.GetCommandName(triggerType), tableName.Unquoted().ToString());

                    String dropTrigger = $"DROP TRIGGER IF EXISTS {triggerName}";

                    command.CommandText = dropTrigger;
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during DropTrigger : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }
        }

        public string CreateDropTriggerScriptText(DbCommandType triggerType)
        {
            var triggerName = string.Format(this.sqliteObjectNames.GetCommandName(triggerType), tableName.Unquoted().ToString());
            string dropTrigger = $"DROP TRIGGER IF EXISTS {triggerName}";
            string str = $"Drop Trigger {triggerName} for table {tableName.Quoted().ToString()}";
            return SqliteTableBuilder.WrapScriptTextWithComments(dropTrigger, str);
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

        public string DropInsertTriggerScriptText()
        {
            return CreateDropTriggerScriptText(DbCommandType.InsertTrigger);
        }

        public string DropUpdateTriggerScriptText()
        {
            return CreateDropTriggerScriptText(DbCommandType.UpdateTrigger);
        }

        public string DropDeleteTriggerScriptText()
        {
            return CreateDropTriggerScriptText(DbCommandType.DeleteTrigger);
        }
    }
}
