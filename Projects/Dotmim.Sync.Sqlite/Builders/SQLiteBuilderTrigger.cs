﻿using Dotmim.Sync.Builders;
using System;
using System.Text;
using Dotmim.Sync.Data;
using System.Data.Common;
using System.Linq;
using Dotmim.Sync.Log;
using System.Data;
using Microsoft.Data.Sqlite;
using Dotmim.Sync.Filter;
using System.Diagnostics;
using System.Collections.Generic;

namespace Dotmim.Sync.Sqlite
{
    public class SqliteBuilderTrigger : IDbBuilderTriggerHelper
    {
        private ObjectNameParser tableName;
        private ObjectNameParser trackingName;
        private DmTable tableDescription;
        private SqliteConnection connection;
        private SqliteTransaction transaction;
        private SqliteObjectNames sqliteObjectNames;

        public ICollection<FilterClause> Filters { get; set; }



        public SqliteBuilderTrigger(DmTable tableDescription, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as SqliteConnection;
            this.transaction = transaction as SqliteTransaction;
            this.tableDescription = tableDescription;
            (this.tableName, this.trackingName) = SqliteBuilder.GetParsers(this.tableDescription);
            this.sqliteObjectNames = new SqliteObjectNames(this.tableDescription);
        }

        private string DeleteTriggerBodyText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("BEGIN");
            stringBuilder.AppendLine($"UPDATE {trackingName.QuotedString} ");
            stringBuilder.AppendLine("SET [sync_row_is_tombstone] = 1");
            stringBuilder.AppendLine("\t,[update_scope_id] = NULL -- since the update if from local, it's a NULL");
            stringBuilder.AppendLine($"\t,[update_timestamp] = {SqliteObjectNames.TimestampValue}");
            stringBuilder.AppendLine($"\t,[timestamp] = {SqliteObjectNames.TimestampValue}");
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
            stringBuilder.Append(SqliteManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, trackingName.QuotedString, "old"));
            stringBuilder.AppendLine(";");
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

            var delTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.DeleteTrigger), tableName.UnquotedStringWithUnderScore);
            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER IF NOT EXISTS {delTriggerName} AFTER DELETE ON {tableName.QuotedString} ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.DeleteTriggerBodyText());

            string str = $"Delete Trigger for table {tableName.QuotedString}";
            return SqliteBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
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
            stringBuilder.AppendLine($"\t\t,{SqliteObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t\t,NULL");
            stringBuilder.AppendLine("\t\t,0");
            stringBuilder.AppendLine($"\t\t,{SqliteObjectNames.TimestampValue}");
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
                using (var command = new SqliteCommand())
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
            var insTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.InsertTrigger), tableName.UnquotedStringWithUnderScore);
            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER IF NOT EXISTS {insTriggerName} AFTER INSERT ON {tableName.QuotedString} ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            string str = $"Insert Trigger for table {tableName.QuotedString}";
            return SqliteBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);

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
            stringBuilder.AppendLine($"\t\t,[update_timestamp] = {SqliteObjectNames.TimestampValue}");
            stringBuilder.AppendLine($"\t\t,[timestamp] = {SqliteObjectNames.TimestampValue}");
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
            stringBuilder.Append(SqliteManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, trackingName.QuotedString, "new"));
            stringBuilder.AppendLine($"; ");
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
            var updTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.UpdateTrigger), tableName.UnquotedStringWithUnderScore);
            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER IF NOT EXISTS {updTriggerName} AFTER UPDATE ON {tableName.QuotedString} ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.UpdateTriggerBodyText());

            string str = $"Update Trigger for table {tableName.QuotedString}";
            return SqliteBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
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
            var updTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.UpdateTrigger), tableName.UnquotedStringWithUnderScore);
            var delTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.DeleteTrigger), tableName.UnquotedStringWithUnderScore);
            var insTriggerName = string.Format(this.sqliteObjectNames.GetCommandName(DbCommandType.InsertTrigger), tableName.UnquotedStringWithUnderScore);

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

                    var triggerName = string.Format(this.sqliteObjectNames.GetCommandName(triggerType), tableName.UnquotedStringWithUnderScore);

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
            var triggerName = string.Format(this.sqliteObjectNames.GetCommandName(triggerType), tableName.UnquotedStringWithUnderScore);
            string dropTrigger = $"DROP TRIGGER IF EXISTS {triggerName}";
            string str = $"Drop Trigger {triggerName} for table {tableName.QuotedString}";
            return SqliteBuilder.WrapScriptTextWithComments(dropTrigger, str);
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
