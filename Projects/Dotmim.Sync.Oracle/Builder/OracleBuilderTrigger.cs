using System;
using System.Data;
using System.Data.Common;
using System.Data.OracleClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Filter;

namespace Dotmim.Sync.Oracle.Builder
{
    internal class OracleBuilderTrigger : IDbBuilderTriggerHelper
    {
        private DmTable tableDescription;
        private OracleConnection connection;
        private OracleTransaction transaction;

        private ObjectNameParser tableName;
        private ObjectNameParser trackingName;
        private OracleObjectNames oracleObjectNames;

        public OracleBuilderTrigger(DmTable tableDescription, DbConnection connection, DbTransaction transaction)
        {
            this.tableDescription = tableDescription;
            this.connection = connection as OracleConnection;
            this.transaction = transaction as OracleTransaction;

            (this.tableName, this.trackingName) = OracleBuilder.GetParsers(this.tableDescription);
            this.oracleObjectNames = new OracleObjectNames(this.tableDescription);
        }

        public FilterClauseCollection Filters { get; set; }

        #region Methods Private

        private string InsertTriggerBodyText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append(OracleBuilderTrigger.CreateBeginTrigger());
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- If row was deleted before, it already exists, so just make an update");
            stringBuilder.AppendLine($"UPDATE {trackingName.QuotedString} ");
            stringBuilder.AppendLine("SET \t[sync_row_is_tombstone] = 0");
            stringBuilder.AppendLine("\t,[update_scope_id] = NULL -- since the update if from local, it's a NULL");
            stringBuilder.AppendLine("\t,[last_change_datetime] = sysdate");
            // Filter columns
            if (this.Filters != null && Filters.Count > 0)
            {
                foreach (var filter in Filters)
                {
                    var columnFilter = this.tableDescription.Columns[filter.ColumnName];
                    if (columnFilter == null)
                        throw new InvalidExpressionException($"Column {filter.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                    if (this.tableDescription.PrimaryKey.Columns.Any(c => c.ColumnName == columnFilter.ColumnName))
                        continue;

                    ObjectNameParser columnName = new ObjectNameParser(columnFilter.ColumnName);

                    stringBuilder.AppendLine($"\t,{columnName.QuotedString} = :new.{columnName.QuotedString}");

                }
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine($"WHERE ");
            stringBuilder.AppendLine(OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, trackingName.QuotedString, ":new"));
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"INSERT INTO {trackingName.QuotedString} (");

            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderArguments2 = new StringBuilder();
            StringBuilder stringPkAreNull = new StringBuilder();

            string argComma = string.Empty;
            string argAnd = string.Empty;
            foreach (var mutableColumn in this.tableDescription.PrimaryKey.Columns.Where(c => !c.ReadOnly))
            {
                ObjectNameParser columnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilderArguments.AppendLine($"\t{argComma} {trackingName.QuotedString}.{columnName.QuotedString}");
                stringBuilderArguments2.AppendLine($"\t{argComma} :new.{columnName.QuotedString}");
                stringPkAreNull.Append($"{argAnd} :new.{columnName.QuotedString} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine("\t,create_scope_id");
            stringBuilder.AppendLine("\t,create_timestamp");
            stringBuilder.AppendLine("\t,update_scope_id");
            stringBuilder.AppendLine("\t,update_timestamp");
            stringBuilder.AppendLine("\t,timestamp");
            stringBuilder.AppendLine("\t,sync_row_is_tombstone");
            stringBuilder.AppendLine("\t,last_change_datetime");

            StringBuilder filterColumnsString = new StringBuilder();

            // Filter columns
            if (this.Filters != null && Filters.Count > 0)
            {
                foreach (var filter in Filters)
                {
                    var columnFilter = this.tableDescription.Columns[filter.ColumnName];
                    if (columnFilter == null)
                        throw new InvalidExpressionException($"Column {filter.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                    if (this.tableDescription.PrimaryKey.Columns.Any(c => c.ColumnName == columnFilter.ColumnName))
                        continue;

                    ObjectNameParser columnName = new ObjectNameParser(columnFilter.ColumnName);
                    filterColumnsString.AppendLine($"\t, :new.{columnName.QuotedString}");
                }

                stringBuilder.AppendLine(filterColumnsString.ToString());
            }
            stringBuilder.AppendLine(") ");
            stringBuilder.AppendLine("VALUES (");
            stringBuilder.Append(stringBuilderArguments2.ToString());
            stringBuilder.AppendLine("\t,NULL");
            stringBuilder.AppendLine($"\t,{OracleObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t,NULL");
            stringBuilder.AppendLine("\t,0");
            stringBuilder.AppendLine($"\t,{OracleObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t,0");
            stringBuilder.AppendLine("\t,sysdate");

            if (Filters != null)
                stringBuilder.AppendLine(filterColumnsString.ToString());

            stringBuilder.AppendLine("\t);");
            stringBuilder.AppendLine(OracleBuilderTrigger.CreateEndTrigger());
            return stringBuilder.ToString();
        }

        private string UpdateTriggerBodyText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append(OracleBuilderTrigger.CreateBeginTrigger());
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"UPDATE {trackingName.QuotedString} ");
            stringBuilder.AppendLine("SET \t[update_scope_id] = NULL -- since the update if from local, it's a NULL");
            stringBuilder.AppendLine($"\t,[update_timestamp] = {OracleObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t,[last_change_datetime] = sysdate");
            // Filter columns
            if (this.Filters != null && Filters.Count > 0)
            {
                foreach (var filter in Filters)
                {
                    var columnFilter = this.tableDescription.Columns[filter.ColumnName];
                    if (columnFilter == null)
                        throw new InvalidExpressionException($"Column {filter.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                    if (this.tableDescription.PrimaryKey.Columns.Any(c => c.ColumnName == columnFilter.ColumnName))
                        continue;

                    ObjectNameParser columnName = new ObjectNameParser(columnFilter.ColumnName);
                    stringBuilder.AppendLine($"\t,{columnName.QuotedString} = :old.{columnName.QuotedString}");
                }
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine($"WHERE ");
            stringBuilder.AppendLine(OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, trackingName.QuotedString, ":old"));
            stringBuilder.Append(";");
            stringBuilder.AppendLine(OracleBuilderTrigger.CreateEndTrigger());
            return stringBuilder.ToString();
        }

        private string DeleteTriggerBodyText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append(OracleBuilderTrigger.CreateBeginTrigger());
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"UPDATE {trackingName.QuotedString} ");
            stringBuilder.AppendLine("SET \t[sync_row_is_tombstone] = 1");
            stringBuilder.AppendLine("\t,[update_scope_id] = NULL -- since the update if from local, it's a NULL");
            stringBuilder.AppendLine($"\t,[update_timestamp] = {OracleObjectNames.TimestampValue}");
            stringBuilder.AppendLine("\t,[last_change_datetime] = sysdate");
            // Filter columns
            if (this.Filters != null && Filters.Count > 0)
            {
                foreach (var filter in this.Filters)
                {
                    var columnFilter = this.tableDescription.Columns[filter.ColumnName];
                    if (columnFilter == null)
                        throw new InvalidExpressionException($"Column {filter.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                    if (this.tableDescription.PrimaryKey.Columns.Any(c => c.ColumnName == columnFilter.ColumnName))
                        continue;

                    ObjectNameParser columnName = new ObjectNameParser(columnFilter.ColumnName);
                    stringBuilder.AppendLine($"\t,{columnName.QuotedString} = :old.{columnName.QuotedString}");

                }
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine($"WHERE ");
            stringBuilder.AppendLine(OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, trackingName.QuotedString, ":old"));
            stringBuilder.Append(";");
            stringBuilder.AppendLine(OracleBuilderTrigger.CreateEndTrigger());
            return stringBuilder.ToString();
        }

        #endregion

        #region Static

        private static string CreateBeginTrigger()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("DECLARE");
            sb.AppendLine("\t-- variable declarations");
            sb.AppendLine("BEGIN");
            sb.AppendLine("\t-- trigger code");
            return sb.ToString();
        }

        private static string CreateEndTrigger()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("END;");
            return sb.ToString();
        }

        #endregion

        #region Delete

        public void CreateDeleteTrigger()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new OracleCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var delTriggerName = this.oracleObjectNames.GetCommandName(DbCommandType.DeleteTrigger);


                    StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER {delTriggerName} AFTER DELETE ON {tableName.QuotedString} FOR EACH ROW ");
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
                using (var command = new OracleCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var delTriggerName = this.oracleObjectNames.GetCommandName(DbCommandType.DeleteTrigger);

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
            var delTriggerName = this.oracleObjectNames.GetCommandName(DbCommandType.DeleteTrigger);
            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER {delTriggerName} AFTER DELETE ON {tableName.QuotedString} FOR EACH ROW ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.DeleteTriggerBodyText());

            string str = $"Delete Trigger for table {tableName.QuotedString}";
            return OracleBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }

        public void AlterDeleteTrigger()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new OracleCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = (OracleTransaction)this.transaction;

                    var delTriggerName = this.oracleObjectNames.GetCommandName(DbCommandType.DeleteTrigger);
                    StringBuilder createTrigger = new StringBuilder($"CREATE OR REPLACE TRIGGER {delTriggerName} AFTER DELETE ON {tableName.QuotedString} FOR EACH ROW ");
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
            (var tableName, var trackingName) = OracleBuilder.GetParsers(this.tableDescription);
            var delTriggerName = this.oracleObjectNames.GetCommandName(DbCommandType.DeleteTrigger);
            StringBuilder createTrigger = new StringBuilder($"CREATE OR REPLACE TRIGGER {delTriggerName} AFTER DELETE ON {tableName.QuotedString} FOR EACH ROW ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            string str = $"ALTER Trigger Delete for table {tableName.QuotedString}";
            return OracleBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }

        public string DropDeleteTriggerScriptText()
        {
            var triggerName = this.oracleObjectNames.GetCommandName(DbCommandType.DeleteTrigger);
            var trigger = $"DELETE TRIGGER {triggerName};";
            var str = $"Drop Delete Trigger for table {tableName.QuotedString}";
            return OracleBuilder.WrapScriptTextWithComments(trigger, str);
        }

        #endregion

        #region Insert

        public void CreateInsertTrigger()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new OracleCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = (OracleTransaction)this.transaction;

                    var insTriggerName = this.oracleObjectNames.GetCommandName(DbCommandType.InsertTrigger);
                    StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER {insTriggerName} AFTER INSERT ON {tableName.QuotedString} FOR EACH ROW ");
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
                using (var command = new OracleCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var triggerName = this.oracleObjectNames.GetCommandName(DbCommandType.InsertTrigger);

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
            var insTriggerName = this.oracleObjectNames.GetCommandName(DbCommandType.InsertTrigger);
            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER {insTriggerName} AFTER INSERT ON {tableName.QuotedString} FOR EACH ROW ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            string str = $"Insert Trigger for table {tableName.QuotedString}";
            return OracleBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);

        }

        public void AlterInsertTrigger()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new OracleCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = (OracleTransaction)this.transaction;

                    var insTriggerName = this.oracleObjectNames.GetCommandName(DbCommandType.InsertTrigger);
                    StringBuilder createTrigger = new StringBuilder($"ALTER TRIGGER {insTriggerName} AFTER INSERT ON {tableName.QuotedString} FOR EACH ROW ");
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
            var insTriggerName = this.oracleObjectNames.GetCommandName(DbCommandType.InsertTrigger);
            StringBuilder createTrigger = new StringBuilder($"ALTER TRIGGER {insTriggerName} AFTER INSERT ON {tableName.QuotedString} FOR EACH ROW ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            string str = $"ALTER Trigger Insert for table {tableName.QuotedString}";
            return OracleBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }

        public string DropInsertTriggerScriptText()
        {
            var triggerName = this.oracleObjectNames.GetCommandName(DbCommandType.InsertTrigger);
            var trigger = $"DELETE TRIGGER {triggerName};";
            var str = $"Drop Insert Trigger for table {tableName.QuotedString}";
            return OracleBuilder.WrapScriptTextWithComments(trigger, str);
        }

        #endregion

        #region Update

        public void CreateUpdateTrigger()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new OracleCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = (OracleTransaction)this.transaction;

                    var updTriggerName = this.oracleObjectNames.GetCommandName(DbCommandType.UpdateTrigger);
                    StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER {updTriggerName} ON {tableName.QuotedString} FOR EACH ROW ");
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
                using (var command = new OracleCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var triggerName = this.oracleObjectNames.GetCommandName(DbCommandType.UpdateTrigger);

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
            var updTriggerName = this.oracleObjectNames.GetCommandName(DbCommandType.UpdateTrigger);
            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER {updTriggerName} ON {tableName.QuotedString} FOR EACH ROW ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.UpdateTriggerBodyText());

            string str = $"Update Trigger for table {tableName.QuotedString}";
            return OracleBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }

        public string DropUpdateTriggerScriptText()
        {
            var triggerName = this.oracleObjectNames.GetCommandName(DbCommandType.UpdateTrigger);
            var trigger = $"DELETE TRIGGER {triggerName};";
            var str = $"Drop Update Trigger for table {tableName.QuotedString}";
            return OracleBuilder.WrapScriptTextWithComments(trigger, str);
        }

        public void AlterUpdateTrigger()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new OracleCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = (OracleTransaction)this.transaction;

                    var updTriggerName = this.oracleObjectNames.GetCommandName(DbCommandType.UpdateTrigger);
                    StringBuilder createTrigger = new StringBuilder($"ALTER TRIGGER {updTriggerName} ON {tableName.QuotedString} FOR EACH ROW ");
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
            (var tableName, var trackingName) = OracleBuilder.GetParsers(this.tableDescription);
            var updTriggerName = this.oracleObjectNames.GetCommandName(DbCommandType.UpdateTrigger);

            StringBuilder createTrigger = new StringBuilder($"ALTER TRIGGER {updTriggerName} ON {tableName.QuotedString} FOR EACH ROW ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            string str = $"ALTER Trigger Update for table {tableName.QuotedString}";
            return OracleBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }

        #endregion

        public bool NeedToCreateTrigger(DbTriggerType type)
        {
            var updTriggerName = this.oracleObjectNames.GetCommandName(DbCommandType.UpdateTrigger);
            var delTriggerName = this.oracleObjectNames.GetCommandName(DbCommandType.DeleteTrigger);
            var insTriggerName = this.oracleObjectNames.GetCommandName(DbCommandType.InsertTrigger);

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

            return !OracleManagementUtils.TriggerExists(connection, transaction, triggerName);
        }
    }
}
