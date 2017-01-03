using Dotmim.Sync.Core.Builders;
using System;
using System.Collections.Generic;
using System.Text;
using Dotmim.Sync.Data;
using System.Data.Common;
using Dotmim.Sync.Core.Common;
using System.Data.SqlClient;
using System.Linq;
using Dotmim.Sync.Core.Log;
using System.Data;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlBuilderTrigger : IDbBuilderTriggerHelper
    {
        private ObjectNameParser tableName;
        private ObjectNameParser trackingName;
        private DmTable tableDescription;
        private SqlConnection connection;
        private SqlTransaction transaction;
        public List<DmColumn> FilterColumns { get; set; }
        public DbObjectNames ObjectNames { get; set; }

        public DmTable TableDescription
        {
            get
            {
                return this.tableDescription;
            }
            set
            {
                this.tableDescription = value;
                (this.tableName, this.trackingName) = SqlBuilder.GetParsers(TableDescription);

            }
        }

        public SqlBuilderTrigger(DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as SqlConnection;
            this.transaction = transaction as SqlTransaction;
        }



        private string DeleteTriggerBodyText(DmTable TableDescription)
        {
            (var tableName, var trackingName) = SqlBuilder.GetParsers(TableDescription);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("UPDATE [side] ");
            stringBuilder.AppendLine("SET \t[sync_row_is_tombstone] = 1");
            stringBuilder.AppendLine("\t,[update_scope_name] = NULL -- since the update if from local, it's a NULL");
            stringBuilder.AppendLine("\t,[update_timestamp] = @@DBTS+1");
            stringBuilder.AppendLine("\t,[last_change_datetime] = GetDate()");
            // Filter columns
            if (this.FilterColumns != null)
            {
                for (int i = 0; i < this.FilterColumns.Count; i++)
                {
                    var filterColumn = this.FilterColumns[i];

                    if (TableDescription.PrimaryKey.Columns.Any(c => c.ColumnName == filterColumn.ColumnName))
                        continue;

                    ObjectNameParser columnName = new ObjectNameParser(filterColumn.ColumnName);

                    stringBuilder.AppendLine($"\t,{columnName.QuotedString} = [d].{columnName.QuotedString}");

                }
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine($"FROM {trackingName.QuotedString} [side]");
            stringBuilder.Append($"JOIN DELETED AS [d] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(TableDescription.PrimaryKey.Columns, "[side]", "[d]"));
            return stringBuilder.ToString();
        }
        public void CreateDeleteTrigger()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = (SqlTransaction)this.transaction;

                    var delTriggerName = string.Format(ObjectNames.GetObjectName(DbObjectType.DeleteTriggerName), tableName.UnquotedStringWithUnderScore);
                    StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER {delTriggerName} ON {tableName.QuotedString} FOR DELETE AS");
                    createTrigger.AppendLine();
                    createTrigger.AppendLine(this.DeleteTriggerBodyText(TableDescription));

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

            var delTriggerName = string.Format(ObjectNames.GetObjectName(DbObjectType.DeleteTriggerName), tableName.UnquotedStringWithUnderScore);
            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER {delTriggerName} ON {tableName.QuotedString} FOR DELETE AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.DeleteTriggerBodyText(TableDescription));

            string str = $"Delete Trigger for table {tableName.QuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }
        public void AlterDeleteTrigger()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = (SqlTransaction)this.transaction;

                    var delTriggerName = string.Format(ObjectNames.GetObjectName(DbObjectType.DeleteTriggerName), tableName.UnquotedStringWithUnderScore);
                    StringBuilder createTrigger = new StringBuilder($"ALTER TRIGGER {delTriggerName} ON {tableName.QuotedString} FOR DELETE AS ");
                    createTrigger.AppendLine();
                    createTrigger.AppendLine(this.DeleteTriggerBodyText(TableDescription));

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
        public string AlterDeleteTriggerScriptText()
        {
            (var tableName, var trackingName) = SqlBuilder.GetParsers(TableDescription);
            var delTriggerName = string.Format(ObjectNames.GetObjectName(DbObjectType.DeleteTriggerName), tableName.UnquotedStringWithUnderScore);
            StringBuilder createTrigger = new StringBuilder($"ALTER TRIGGER {delTriggerName} ON {tableName.QuotedString} FOR DELETE AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText(TableDescription));

            string str = $"ALTER Trigger Delete for table {tableName.QuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }


        private string InsertTriggerBodyText(DmTable TableDescription)
        {
            (var tableName, var trackingName) = SqlBuilder.GetParsers(TableDescription);
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- If row was deleted before, it already exists, so just make an update");
            stringBuilder.AppendLine("UPDATE [side] ");
            stringBuilder.AppendLine("SET \t[sync_row_is_tombstone] = 0");
            stringBuilder.AppendLine("\t,[update_scope_name] = NULL -- since the update if from local, it's a NULL");
            stringBuilder.AppendLine("\t,[last_change_datetime] = GetDate()");
            // Filter columns
            if (this.FilterColumns != null)
            {
                for (int i = 0; i < this.FilterColumns.Count; i++)
                {
                    var filterColumn = this.FilterColumns[i];

                    if (TableDescription.PrimaryKey.Columns.Any(c => c.ColumnName == filterColumn.ColumnName))
                        continue;

                    ObjectNameParser columnName = new ObjectNameParser(filterColumn.ColumnName);

                    stringBuilder.AppendLine($"\t,{columnName.QuotedString} = [i].{columnName.QuotedString}");

                }
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine($"FROM {trackingName.QuotedString} [side]");
            stringBuilder.Append($"JOIN INSERTED AS [i] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(TableDescription.PrimaryKey.Columns, "[side]", "[i]"));
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"INSERT INTO {trackingName.QuotedString} (");

            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringPkAreNull = new StringBuilder();

            string argComma = string.Empty;
            string argAnd = string.Empty;
            foreach (var mutableColumn in TableDescription.PrimaryKey.Columns)
            {
                ObjectNameParser columnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilderArguments.AppendLine($"\t{argComma}[i].{columnName.QuotedString}");
                stringPkAreNull.Append($"{argAnd}[side].{columnName.QuotedString} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine("\t,[create_scope_name]");
            stringBuilder.AppendLine("\t,[create_timestamp]");
            stringBuilder.AppendLine("\t,[update_scope_name]");
            stringBuilder.AppendLine("\t,[update_timestamp]");
            stringBuilder.AppendLine("\t,[sync_row_is_tombstone]");
            stringBuilder.AppendLine("\t,[last_change_datetime]");

            StringBuilder filterColumnsString = new StringBuilder();
            // Filter columns
            if (this.FilterColumns != null)
            {
                for (int i = 0; i < this.FilterColumns.Count; i++)
                {
                    var filterColumn = this.FilterColumns[i];

                    if (TableDescription.PrimaryKey.Columns.Any(c => c.ColumnName == filterColumn.ColumnName))
                        continue;

                    ObjectNameParser columnName = new ObjectNameParser(filterColumn.ColumnName);

                    filterColumnsString.AppendLine($"\t,[i].{columnName.QuotedString}");

                }

                stringBuilder.AppendLine(filterColumnsString.ToString());
            }
            stringBuilder.AppendLine(") ");
            stringBuilder.AppendLine("SELECT");
            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine("\t,NULL");
            stringBuilder.AppendLine("\t,@@DBTS+1");
            stringBuilder.AppendLine("\t,NULL");
            stringBuilder.AppendLine("\t,@@DBTS+1");
            stringBuilder.AppendLine("\t,0");
            stringBuilder.AppendLine("\t,GetDate()");

            if (FilterColumns != null)
                stringBuilder.AppendLine(filterColumnsString.ToString());

            stringBuilder.AppendLine("FROM INSERTED [i]");
            stringBuilder.Append($"LEFT JOIN {trackingName.QuotedString} [side] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(TableDescription.PrimaryKey.Columns, "[i]", "[side]"));
            stringBuilder.Append("WHERE ");
            stringBuilder.AppendLine(stringPkAreNull.ToString());
            return stringBuilder.ToString();
        }
        public void CreateInsertTrigger()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = (SqlTransaction)this.transaction;

                    var insTriggerName = string.Format(ObjectNames.GetObjectName(DbObjectType.InsertTriggerName), tableName.UnquotedStringWithUnderScore);
                    StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER {insTriggerName} ON {tableName.QuotedString} FOR INSERT AS");
                    createTrigger.AppendLine();
                    createTrigger.AppendLine(this.InsertTriggerBodyText(TableDescription));

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
            var insTriggerName = string.Format(ObjectNames.GetObjectName(DbObjectType.InsertTriggerName), tableName.UnquotedStringWithUnderScore);
            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER {insTriggerName} ON {tableName.QuotedString} FOR INSERT AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText(TableDescription));

            string str = $"Insert Trigger for table {tableName.QuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);

        }
        public void AlterInsertTrigger()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = (SqlTransaction)this.transaction;

                    var insTriggerName = string.Format(ObjectNames.GetObjectName(DbObjectType.InsertTriggerName), tableName.UnquotedStringWithUnderScore);
                    StringBuilder createTrigger = new StringBuilder($"ALTER TRIGGER {insTriggerName} ON {tableName.QuotedString} FOR INSERT AS ");
                    createTrigger.AppendLine();
                    createTrigger.AppendLine(this.InsertTriggerBodyText(TableDescription));

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
        public string AlterInsertTriggerScriptText()
        {
            var insTriggerName = string.Format(ObjectNames.GetObjectName(DbObjectType.InsertTriggerName), tableName.UnquotedStringWithUnderScore);
            StringBuilder createTrigger = new StringBuilder($"ALTER TRIGGER {insTriggerName} ON {tableName.QuotedString} FOR INSERT AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText(TableDescription));

            string str = $"ALTER Trigger Insert for table {tableName.QuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }


        private string UpdateTriggerBodyText(DmTable TableDescription)
        {
            (var tableName, var trackingName) = SqlBuilder.GetParsers(TableDescription);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("UPDATE [side] ");
            stringBuilder.AppendLine("SET \t[update_scope_name] = NULL -- since the update if from local, it's a NULL");
            stringBuilder.AppendLine("\t,[update_timestamp] = @@DBTS+1");
            stringBuilder.AppendLine("\t,[last_change_datetime] = GetDate()");
            // Filter columns
            if (this.FilterColumns != null)
            {
                for (int i = 0; i < this.FilterColumns.Count; i++)
                {
                    var filterColumn = this.FilterColumns[i];

                    if (TableDescription.PrimaryKey.Columns.Any(c => c.ColumnName == filterColumn.ColumnName))
                        continue;

                    ObjectNameParser columnName = new ObjectNameParser(filterColumn.ColumnName);

                    stringBuilder.AppendLine($"\t,{columnName.QuotedString} = [i].{columnName.QuotedString}");

                }
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine($"FROM {trackingName.QuotedString} [side]");
            stringBuilder.Append($"JOIN INSERTED AS [i] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(TableDescription.PrimaryKey.Columns, "[side]", "[i]"));
            return stringBuilder.ToString();
        }
        public void CreateUpdateTrigger()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = (SqlTransaction)this.transaction;

                    var updTriggerName = string.Format(ObjectNames.GetObjectName(DbObjectType.UpdateTriggerName), tableName.UnquotedStringWithUnderScore);
                    StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER {updTriggerName} ON {tableName.QuotedString} FOR UPDATE AS");
                    createTrigger.AppendLine();
                    createTrigger.AppendLine(this.UpdateTriggerBodyText(TableDescription));

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
            var updTriggerName = string.Format(ObjectNames.GetObjectName(DbObjectType.UpdateTriggerName), tableName.UnquotedStringWithUnderScore);
            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER {updTriggerName} ON {tableName.QuotedString} FOR UPDATE AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.UpdateTriggerBodyText(TableDescription));

            string str = $"Update Trigger for table {tableName.QuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }
        public void AlterUpdateTrigger()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = (SqlTransaction)this.transaction;

                    var updTriggerName = string.Format(ObjectNames.GetObjectName(DbObjectType.UpdateTriggerName), tableName.UnquotedStringWithUnderScore);
                    StringBuilder createTrigger = new StringBuilder($"ALTER TRIGGER {updTriggerName} ON {tableName.QuotedString} FOR UPDATE AS ");
                    createTrigger.AppendLine();
                    createTrigger.AppendLine(this.UpdateTriggerBodyText(TableDescription));

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
        public string AlterUpdateTriggerScriptText()
        {
            (var tableName, var trackingName) = SqlBuilder.GetParsers(TableDescription);
            var updTriggerName = string.Format(ObjectNames.GetObjectName(DbObjectType.UpdateTriggerName), tableName.UnquotedStringWithUnderScore);

            StringBuilder createTrigger = new StringBuilder($"ALTER TRIGGER {updTriggerName} ON {tableName.QuotedString} FOR UPDATE AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText(TableDescription));

            string str = $"ALTER Trigger Update for table {tableName.QuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }
        public bool NeedToCreateTrigger(DbTriggerType type, DbBuilderOption option)
        {
    
            var updTriggerName = string.Format(ObjectNames.GetObjectName(DbObjectType.UpdateTriggerName), tableName.UnquotedStringWithUnderScore);
            var delTriggerName = string.Format(ObjectNames.GetObjectName(DbObjectType.DeleteTriggerName), tableName.UnquotedStringWithUnderScore);
            var insTriggerName = string.Format(ObjectNames.GetObjectName(DbObjectType.InsertTriggerName), tableName.UnquotedStringWithUnderScore);

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

                return !SqlManagementUtils.TriggerExists(connection, transaction, triggerName);

            }

            if (option.HasFlag(DbBuilderOption.UseExistingSchema))
                return false;

            return false;
        }


    }
}
