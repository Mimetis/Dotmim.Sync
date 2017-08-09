using Dotmim.Sync.Builders;
using System;
using System.Text;
using Dotmim.Sync.Data;
using System.Data.Common;
using System.Linq;
using Dotmim.Sync.Log;
using System.Data;
using System.Data.SQLite;
using Dotmim.Sync.Filter;

namespace Dotmim.Sync.SQLite
{
    public class SQLiteBuilderTrigger : IDbBuilderTriggerHelper
    {
        private ObjectNameParser tableName;
        private ObjectNameParser trackingName;
        private DmTable tableDescription;
        private SQLiteConnection connection;
        private SQLiteTransaction transaction;
        private SQLiteObjectNames sqliteObjectNames;

        public FilterClauseCollection Filters { get; set; }

    

        public SQLiteBuilderTrigger(DmTable tableDescription, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as SQLiteConnection;
            this.transaction = transaction as SQLiteTransaction;
            this.tableDescription = tableDescription;
            (this.tableName, this.trackingName) = SQLiteBuilder.GetParsers(this.tableDescription);
            this.sqliteObjectNames = new SQLiteObjectNames(this.tableDescription);
        }

        private string DeleteTriggerBodyText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("BEGIN");
            stringBuilder.AppendLine($"UPDATE {trackingName.QuotedString} ");
            stringBuilder.AppendLine("SET [sync_row_is_tombstone] = 1");
            stringBuilder.AppendLine("\t,[update_scope_id] = NULL -- since the update if from local, it's a NULL");
            stringBuilder.AppendLine($"\t,[update_timestamp] = {SQLiteObjectNames.TimestampValue}");
            stringBuilder.AppendLine($"\t,[timestamp] = {SQLiteObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t,[last_change_datetime] = datetime('now')");

            // --------------------------------------------------------------------------------
            // SQLITE doesnot support (yet) filtering columns, since it's only a client provider
            // --------------------------------------------------------------------------------
            //// Filter columns
            //if (this.Filters != null)
            //{
            //    for (int i = 0; i < this.Filters.Count; i++)
            //    {
            //        var filterColumn = this.Filters[i];

            //        if (this.tableDescription.PrimaryKey.Columns.Any(c => c.ColumnName == filterColumn.ColumnName))
            //            continue;

            //        ObjectNameParser columnName = new ObjectNameParser(filterColumn.ColumnName);

            //        stringBuilder.AppendLine($"\t,{columnName.QuotedString} = [d].{columnName.QuotedString}");

            //    }
            //    stringBuilder.AppendLine();
            //}

            stringBuilder.Append($"WHERE ");
            stringBuilder.Append(SQLiteManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, trackingName.QuotedString, "old"));
            stringBuilder.AppendLine(";");
            stringBuilder.AppendLine("END;");
            return stringBuilder.ToString();
        }
        public void CreateDeleteTrigger()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SQLiteCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var delTriggerName = this.sqliteObjectNames.GetCommandName(DbCommandType.DeleteTrigger);
                    StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER IF NOT EXISTS {delTriggerName} AFTER DELETE ON {tableName.QuotedString} ");
                    createTrigger.AppendLine();
                    createTrigger.AppendLine(this.DeleteTriggerBodyText());

                    command.CommandText = createTrigger.ToString();
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Logger.Current.Error($"Error during CreateDeleteTrigger : {ex}");
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

            var delTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.DeleteTrigger), tableName.UnquotedStringWithUnderScore);
            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER IF NOT EXISTS {delTriggerName} AFTER DELETE ON {tableName.QuotedString} ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.DeleteTriggerBodyText());

            string str = $"Delete Trigger for table {tableName.QuotedString}";
            return SQLiteBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
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
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- If row was deleted before, it already exists, so just make an update");
            stringBuilder.AppendLine("BEGIN");
 
            stringBuilder.AppendLine($"\tINSERT OR REPLACE INTO {trackingName.QuotedString} (");

            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderArguments2 = new StringBuilder();
            StringBuilder stringPkAreNull = new StringBuilder();

            string argComma = string.Empty;
            string argAnd = string.Empty;
            foreach (var mutableColumn in this.tableDescription.PrimaryKey.Columns.Where(c => !c.ReadOnly))
            {
                ObjectNameParser columnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilderArguments.AppendLine($"\t\t{argComma}{columnName.QuotedString}");
                stringBuilderArguments2.AppendLine($"\t\t{argComma}new.{columnName.QuotedString}");
                stringPkAreNull.Append($"{argAnd}{trackingName.QuotedString}.{columnName.QuotedString} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine("\t\t,[create_scope_id]");
            stringBuilder.AppendLine("\t\t,[create_timestamp]");
            stringBuilder.AppendLine("\t\t,[update_scope_id]");
            stringBuilder.AppendLine("\t\t,[update_timestamp]");
            stringBuilder.AppendLine("\t\t,[timestamp]");
            stringBuilder.AppendLine("\t\t,[sync_row_is_tombstone]");
            stringBuilder.AppendLine("\t\t,[last_change_datetime]");

            StringBuilder filterColumnsString = new StringBuilder();

            // --------------------------------------------------------------------------------
            // SQLITE doesnot support (yet) filtering columns, since it's only a client provider
            // --------------------------------------------------------------------------------
            //// Filter columns
            //if (this.Filters != null && this.Filters.Count > 0)
            //{
            //    for (int i = 0; i < this.Filters.Count; i++)
            //    {
            //        var filterColumn = this.Filters[i];
            //        if (this.tableDescription.PrimaryKey.Columns.Any(c => c.ColumnName == filterColumn.ColumnName))
            //            continue;

            //        ObjectNameParser columnName = new ObjectNameParser(filterColumn.ColumnName);
            //        filterColumnsString.AppendLine($"\t,[i].{columnName.QuotedString}");
            //    }
            //    stringBuilder.AppendLine(filterColumnsString.ToString());
            //}

            stringBuilder.AppendLine("\t) ");
            stringBuilder.AppendLine("\tVALUES (");
            stringBuilder.Append(stringBuilderArguments2.ToString());
            stringBuilder.AppendLine("\t\t,NULL");
            stringBuilder.AppendLine($"\t\t,{SQLiteObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t\t,NULL");
            stringBuilder.AppendLine("\t\t,0");
            stringBuilder.AppendLine($"\t\t,{SQLiteObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t\t,0");
            stringBuilder.AppendLine("\t\t,datetime('now')");

            if (Filters != null && Filters.Count > 0)
                stringBuilder.AppendLine(filterColumnsString.ToString());

            stringBuilder.AppendLine("\t);");
            stringBuilder.AppendLine("END;");
            return stringBuilder.ToString();
        }
        public void CreateInsertTrigger()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SQLiteCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var insTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.InsertTrigger), tableName.UnquotedStringWithUnderScore);

                    StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER IF NOT EXISTS {insTriggerName} AFTER INSERT ON {tableName.QuotedString} ");
                    createTrigger.AppendLine();
                    createTrigger.AppendLine(this.InsertTriggerBodyText());

                    command.CommandText = createTrigger.ToString();
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Logger.Current.Error($"Error during CreateDeleteTrigger : {ex}");
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
            var insTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.InsertTrigger), tableName.UnquotedStringWithUnderScore);
            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER IF NOT EXISTS {insTriggerName} AFTER INSERT ON {tableName.QuotedString} ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            string str = $"Insert Trigger for table {tableName.QuotedString}";
            return SQLiteBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);

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
            stringBuilder.AppendLine($"\tUPDATE {trackingName.QuotedString} ");
            stringBuilder.AppendLine("\tSET [update_scope_id] = NULL -- since the update if from local, it's a NULL");
            stringBuilder.AppendLine($"\t\t,[update_timestamp] = {SQLiteObjectNames.TimestampValue}");
            stringBuilder.AppendLine($"\t\t,[timestamp] = {SQLiteObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t\t,[last_change_datetime] = datetime('now')");

            // --------------------------------------------------------------------------------
            // SQLITE doesnot support (yet) filtering columns, since it's only a client provider
            // --------------------------------------------------------------------------------
            // Filter columns
            //if (this.Filters != null && Filters.Count > 0)
            //{
            //    for (int i = 0; i < this.Filters.Count; i++)
            //    {
            //        var filterColumn = this.Filters[i];

            //        if (this.tableDescription.PrimaryKey.Columns.Any(c => c.ColumnName == filterColumn.ColumnName))
            //            continue;

            //        ObjectNameParser columnName = new ObjectNameParser(filterColumn.ColumnName);

            //        stringBuilder.AppendLine($"\t,{columnName.QuotedString} = [i].{columnName.QuotedString}");

            //    }
            //    stringBuilder.AppendLine();
            //}

            stringBuilder.Append($"\tWhere ");
            stringBuilder.Append(SQLiteManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, trackingName.QuotedString, "new"));
            stringBuilder.AppendLine($"; ");
            stringBuilder.AppendLine($"End; ");
            return stringBuilder.ToString();
        }
        public void CreateUpdateTrigger()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SQLiteCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var updTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.UpdateTrigger), tableName.UnquotedStringWithUnderScore);
                    StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER IF NOT EXISTS {updTriggerName} AFTER UPDATE ON {tableName.QuotedString} ");
                    createTrigger.AppendLine();
                    createTrigger.AppendLine(this.UpdateTriggerBodyText());

                    command.CommandText = createTrigger.ToString();
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Logger.Current.Error($"Error during CreateDeleteTrigger : {ex}");
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
            var updTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.UpdateTrigger), tableName.UnquotedStringWithUnderScore);
            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER IF NOT EXISTS {updTriggerName} AFTER UPDATE ON {tableName.QuotedString} ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.UpdateTriggerBodyText());

            string str = $"Update Trigger for table {tableName.QuotedString}";
            return SQLiteBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }
        public void AlterUpdateTrigger()
        {
            return;
        }
        public string AlterUpdateTriggerScriptText()
        {
            return string.Empty;
        }
        public bool NeedToCreateTrigger(DbTriggerType type, DbBuilderOption option)
        {
            var updTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.UpdateTrigger), tableName.UnquotedStringWithUnderScore);
            var delTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.DeleteTrigger), tableName.UnquotedStringWithUnderScore);
            var insTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.InsertTrigger), tableName.UnquotedStringWithUnderScore);

            if (option.HasFlag(DbBuilderOption.CreateOrUseExistingSchema))
            {
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

                return !SQLiteManagementUtils.TriggerExists(connection, transaction, triggerName);

            }

            if (option.HasFlag(DbBuilderOption.UseExistingSchema))
                return false;

            return false;
        }


    }
}
