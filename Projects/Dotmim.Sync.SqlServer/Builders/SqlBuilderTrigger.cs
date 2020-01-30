using Dotmim.Sync.Builders;


using Dotmim.Sync.Log;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlBuilderTrigger : IDbBuilderTriggerHelper
    {
        private ParserName tableName;
        private ParserName trackingName;
        private readonly SyncTable tableDescription;
        private readonly SqlConnection connection;
        private readonly SqlTransaction transaction;
        private readonly SqlObjectNames sqlObjectNames;
        public SyncFilter Filter { get; set; }


        public SqlBuilderTrigger(SyncTable tableDescription, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as SqlConnection;
            this.transaction = transaction as SqlTransaction;

            this.tableDescription = tableDescription;
            (this.tableName, this.trackingName) = SqlTableBuilder.GetParsers(this.tableDescription);
            this.sqlObjectNames = new SqlObjectNames(this.tableDescription);

        }



        private string DeleteTriggerBodyText()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("UPDATE [side] ");
            stringBuilder.AppendLine("SET  [sync_row_is_tombstone] = 0");
            stringBuilder.AppendLine("\t,[update_scope_id] = NULL -- scope id is always NULL when update is made locally");
            stringBuilder.AppendLine("\t,[last_change_datetime] = GetUtcDate()");
            stringBuilder.AppendLine($"FROM {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.Append($"JOIN DELETED AS [d] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[side]", "[d]"));
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"INSERT INTO {trackingName.Schema().Quoted().ToString()} (");

            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();

            string argComma = " ";
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilderArguments.AppendLine($"\t{argComma}[d].{columnName}");
                stringBuilderArguments2.AppendLine($"\t{argComma}{columnName}");
                stringPkAreNull.Append($"{argAnd}[side].{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(stringBuilderArguments2.ToString());
            stringBuilder.AppendLine("\t,[update_scope_id]");
            stringBuilder.AppendLine("\t,[sync_row_is_tombstone]");
            stringBuilder.AppendLine("\t,[last_change_datetime]");
            stringBuilder.AppendLine(") ");
            stringBuilder.AppendLine("SELECT");
            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine("\t,NULL");
            stringBuilder.AppendLine("\t,0");
            stringBuilder.AppendLine("\t,GetUtcDate()");
            stringBuilder.AppendLine("FROM DELETED [d]");
            stringBuilder.Append($"LEFT JOIN {trackingName.Schema().Quoted().ToString()} [side] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[d]", "[side]"));
            stringBuilder.Append("WHERE ");
            stringBuilder.AppendLine(stringPkAreNull.ToString());
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
                        command.Transaction = this.transaction;

                    var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger).name;


                    var createTrigger = new StringBuilder($"CREATE TRIGGER {delTriggerName} ON {tableName.Schema().Quoted().ToString()} FOR DELETE AS");
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
        public void DropDeleteTrigger()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger).name;

                    command.CommandText = $"DROP TRIGGER {delTriggerName};";
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during DropDeleteTrigger : {ex}");
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

            var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger).name;
            var createTrigger = new StringBuilder($"CREATE TRIGGER {delTriggerName} ON {tableName.Schema().Quoted().ToString()} FOR DELETE AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.DeleteTriggerBodyText());

            string str = $"Delete Trigger for table {tableName.Schema().Quoted().ToString()}";
            return SqlTableBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
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

                    var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger).name;
                    var createTrigger = new StringBuilder($"ALTER TRIGGER {delTriggerName} ON {tableName.Schema().Quoted().ToString()} FOR DELETE AS ");
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

        public string AlterDeleteTriggerScriptText()
        {
            (var tableName, _) = SqlTableBuilder.GetParsers(this.tableDescription);
            var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger).name;
            StringBuilder createTrigger = new StringBuilder($"ALTER TRIGGER {delTriggerName} ON {tableName.Schema().Quoted().ToString()} FOR DELETE AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            string str = $"ALTER Trigger Delete for table {tableName.Schema().Quoted().ToString()}";
            return SqlTableBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }

        public string DropDeleteTriggerScriptText()
        {
            var triggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger).name;
            var trigger = $"DELETE TRIGGER {triggerName};";
            var str = $"Drop Delete Trigger for table {tableName.Schema().Quoted().ToString()}";
            return SqlTableBuilder.WrapScriptTextWithComments(trigger, str);
        }

        private string InsertTriggerBodyText()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- If row was deleted before, it already exists, so just make an update");
            stringBuilder.AppendLine("UPDATE [side] ");
            stringBuilder.AppendLine("SET  [sync_row_is_tombstone] = 0");
            stringBuilder.AppendLine("\t,[update_scope_id] = NULL -- scope id is always NULL when update is made locally");
            stringBuilder.AppendLine("\t,[last_change_datetime] = GetUtcDate()");
            stringBuilder.AppendLine($"FROM {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.Append($"JOIN INSERTED AS [i] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[side]", "[i]"));
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"INSERT INTO {trackingName.Schema().Quoted().ToString()} (");

            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();

            string argComma = " ";
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilderArguments.AppendLine($"\t{argComma}[i].{columnName}");
                stringBuilderArguments2.AppendLine($"\t{argComma}{columnName}");
                stringPkAreNull.Append($"{argAnd}[side].{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(stringBuilderArguments2.ToString());
            stringBuilder.AppendLine("\t,[update_scope_id]");
            stringBuilder.AppendLine("\t,[sync_row_is_tombstone]");
            stringBuilder.AppendLine("\t,[last_change_datetime]");
            stringBuilder.AppendLine(") ");
            stringBuilder.AppendLine("SELECT");
            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine("\t,NULL");
            stringBuilder.AppendLine("\t,0");
            stringBuilder.AppendLine("\t,GetUtcDate()");
            stringBuilder.AppendLine("FROM INSERTED [i]");
            stringBuilder.Append($"LEFT JOIN {trackingName.Schema().Quoted().ToString()} [side] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[i]", "[side]"));
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
                        command.Transaction = this.transaction;

                    var insTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger).name;
                    var createTrigger = new StringBuilder($"CREATE TRIGGER {insTriggerName} ON {tableName.Schema().Quoted().ToString()} FOR INSERT AS");
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

        public void DropInsertTrigger()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var triggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger).name;

                    command.CommandText = $"DROP TRIGGER {triggerName};";
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during DropDeleteTrigger : {ex}");
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
            var insTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger).name;
            var createTrigger = new StringBuilder($"CREATE TRIGGER {insTriggerName} ON {tableName.Schema().Quoted().ToString()} FOR INSERT AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            string str = $"Insert Trigger for table {tableName.Schema().Quoted().ToString()}";
            return SqlTableBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);

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

                    var insTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger).name;
                    var createTrigger = new StringBuilder($"ALTER TRIGGER {insTriggerName} ON {tableName.Schema().Quoted().ToString()} FOR INSERT AS ");
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
        public string AlterInsertTriggerScriptText()
        {
            var insTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger).name;
            StringBuilder createTrigger = new StringBuilder($"ALTER TRIGGER {insTriggerName} ON {tableName.Schema().Quoted().ToString()} FOR INSERT AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            string str = $"ALTER Trigger Insert for table {tableName.Schema().Quoted().ToString()}";
            return SqlTableBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }

        public string DropInsertTriggerScriptText()
        {
            var triggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger).name;
            var trigger = $"DELETE TRIGGER {triggerName};";
            var str = $"Drop Insert Trigger for table {tableName.Schema().Quoted().ToString()}";
            return SqlTableBuilder.WrapScriptTextWithComments(trigger, str);
        }
        private string UpdateTriggerBodyText()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("UPDATE [side] ");
            stringBuilder.AppendLine("SET \t[update_scope_id] = NULL -- since the update if from local, it's a NULL");
            stringBuilder.AppendLine("\t,[last_change_datetime] = GetUtcDate()");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"FROM {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.Append($"JOIN INSERTED AS [i] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[side]", "[i]"));

            if (this.tableDescription.GetMutableColumns().Count() > 0)
            {
                stringBuilder.Append($"JOIN DELETED AS [d] ON ");
                stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[d]", "[i]"));

                stringBuilder.AppendLine("WHERE (");
                string or = "";
                foreach (var column in this.tableDescription.GetMutableColumns())
                {
                    var quotedColumn = ParserName.Parse(column).Quoted().ToString();

                    stringBuilder.Append("\t");
                    stringBuilder.Append(or);
                    stringBuilder.Append("ISNULL(");
                    stringBuilder.Append("NULLIF(");
                    stringBuilder.Append("[d].");
                    stringBuilder.Append(quotedColumn);
                    stringBuilder.Append(", ");
                    stringBuilder.Append("[i].");
                    stringBuilder.Append(quotedColumn);
                    stringBuilder.Append(")");
                    stringBuilder.Append(", ");
                    stringBuilder.Append("NULLIF(");
                    stringBuilder.Append("[i].");
                    stringBuilder.Append(quotedColumn);
                    stringBuilder.Append(", ");
                    stringBuilder.Append("[d].");
                    stringBuilder.Append(quotedColumn);
                    stringBuilder.Append(")");
                    stringBuilder.AppendLine(") IS NOT NULL");

                    or = " OR ";
                }
                stringBuilder.AppendLine(") ");
            }

            stringBuilder.AppendLine($"INSERT INTO {trackingName.Schema().Quoted().ToString()} (");

            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();

            string argComma = " ";
            string argAnd = string.Empty;
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            foreach (var mutableColumn in primaryKeys.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilderArguments.AppendLine($"\t{argComma}[i].{columnName}");
                stringBuilderArguments2.AppendLine($"\t{argComma}{columnName}");
                stringPkAreNull.Append($"{argAnd}[side].{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(stringBuilderArguments2.ToString());
            stringBuilder.AppendLine("\t,[update_scope_id]");
            stringBuilder.AppendLine("\t,[sync_row_is_tombstone]");
            stringBuilder.AppendLine("\t,[last_change_datetime]");
            stringBuilder.AppendLine(") ");
            stringBuilder.AppendLine("SELECT");
            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine("\t,NULL");
            stringBuilder.AppendLine("\t,0");
            stringBuilder.AppendLine("\t,GetUtcDate()");
            stringBuilder.AppendLine("FROM INSERTED [i]");
            stringBuilder.Append($"LEFT JOIN {trackingName.Schema().Quoted().ToString()} [side] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[i]", "[side]"));
            stringBuilder.Append("WHERE ");
            stringBuilder.AppendLine(stringPkAreNull.ToString());

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

                    var updTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger).name;
                    var createTrigger = new StringBuilder($"CREATE TRIGGER {updTriggerName} ON {tableName.Schema().Quoted().ToString()} FOR UPDATE AS");
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

        public void DropUpdateTrigger()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var triggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger).name;

                    command.CommandText = $"DROP TRIGGER {triggerName};";
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during DropDeleteTrigger : {ex}");
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
            var updTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger).name;
            var createTrigger = new StringBuilder($"CREATE TRIGGER {updTriggerName} ON {tableName.Schema().Quoted().ToString()} FOR UPDATE AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.UpdateTriggerBodyText());

            string str = $"Update Trigger for table {tableName.Schema().Quoted().ToString()}";
            return SqlTableBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }

        public string DropUpdateTriggerScriptText()
        {
            var triggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger).name;
            var trigger = $"DELETE TRIGGER {triggerName};";
            var str = $"Drop Update Trigger for table {tableName.Schema().Quoted().ToString()}";
            return SqlTableBuilder.WrapScriptTextWithComments(trigger, str);
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
                        command.Transaction = this.transaction;

                    var updTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger).name;
                    var createTrigger = new StringBuilder($"ALTER TRIGGER {updTriggerName} ON {tableName.Schema().Quoted().ToString()} FOR UPDATE AS ");
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
        public string AlterUpdateTriggerScriptText()
        {
            (var tableName, _) = SqlTableBuilder.GetParsers(this.tableDescription);
            var updTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger).name;

            StringBuilder createTrigger = new StringBuilder($"ALTER TRIGGER {updTriggerName} ON {tableName.Schema().Quoted().ToString()} FOR UPDATE AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            string str = $"ALTER Trigger Update for table {tableName.Schema().Quoted().ToString()}";
            return SqlTableBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }

        public virtual bool NeedToCreateTrigger(DbTriggerType type)
        {

            var updTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger).name;
            var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger).name;
            var insTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger).name;

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


    }
}
