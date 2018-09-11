using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Filter;
using Dotmim.Sync.Log;
using Dotmim.Sync.SqlServer.Manager;
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
        private ObjectNameParser tableName;
        private ObjectNameParser trackingName;
        private readonly DmTable tableDescription;
        private readonly SqlConnection connection;
        private readonly SqlTransaction transaction;
        private readonly SqlObjectNames sqlObjectNames;
        private readonly SqlDbMetadata sqlDbMetadata;

        public IList<FilterClause2> Filters { get; set; }


        public SqlBuilderTrigger(DmTable tableDescription, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as SqlConnection;
            this.transaction = transaction as SqlTransaction;

            this.tableDescription = tableDescription;
            (this.tableName, this.trackingName) = SqlBuilder.GetParsers(this.tableDescription);
            this.sqlObjectNames = new SqlObjectNames(this.tableDescription);
            this.sqlDbMetadata = new SqlDbMetadata();

        }

        private string DeleteTriggerBodyText()
        {
            List<DmColumn> addedColumns = new List<DmColumn>();

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("UPDATE [side] ");
            stringBuilder.AppendLine("SET \t[sync_row_is_tombstone] = 1");
            stringBuilder.AppendLine("\t,[update_scope_id] = NULL -- since the update if from local, it's a NULL");
            stringBuilder.AppendLine("\t,[update_timestamp] = @@DBTS+1");
            stringBuilder.AppendLine("\t,[last_change_datetime] = GetUtcDate()");

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

                    var quotedColumnName = new ObjectNameParser(c.ColumnName, "[", "]").FullQuotedString;
                    stringBuilder.AppendLine($"\t,{quotedColumnName} = [d].{quotedColumnName}");

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

                    var quotedColumnName = new ObjectNameParser(columnFilter.ColumnName, "[", "]").FullQuotedString;
                    stringBuilder.AppendLine($"\t,{quotedColumnName} = [d].{quotedColumnName}");

                    addedColumns.Add(columnFilter);
                }
            }
            stringBuilder.AppendLine($"FROM {trackingName.FullQuotedString} [side]");
            stringBuilder.Append($"JOIN DELETED AS [d] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "[side]", "[d]"));

            addedColumns.Clear();
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

                    var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger);


                    StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER {delTriggerName} ON {tableName.FullQuotedString} FOR DELETE AS");
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

                    var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger);

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

            var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger);
            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER {delTriggerName} ON {tableName.FullQuotedString} FOR DELETE AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.DeleteTriggerBodyText());

            string str = $"Delete Trigger for table {tableName.FullQuotedString}";
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

                    var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger);
                    StringBuilder createTrigger = new StringBuilder($"ALTER TRIGGER {delTriggerName} ON {tableName.FullQuotedString} FOR DELETE AS ");
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
            (var tableName, var trackingName) = SqlBuilder.GetParsers(this.tableDescription);
            var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger);
            StringBuilder createTrigger = new StringBuilder($"ALTER TRIGGER {delTriggerName} ON {tableName.FullQuotedString} FOR DELETE AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            string str = $"ALTER Trigger Delete for table {tableName.FullQuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }
        public string DropDeleteTriggerScriptText()
        {
            var triggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger);
            var trigger = $"DELETE TRIGGER {triggerName};";
            var str = $"Drop Delete Trigger for table {tableName.FullQuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(trigger, str);
        }

        private string InsertTriggerBodyText()
        {
            List<DmColumn> addedColumns = new List<DmColumn>();

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- If row was deleted before, it already exists, so just make an update");
            stringBuilder.AppendLine("UPDATE [side] ");
            stringBuilder.AppendLine("SET \t[sync_row_is_tombstone] = 0");
            stringBuilder.AppendLine("\t,[update_scope_id] = NULL -- since the update if from local, it's a NULL");
            stringBuilder.AppendLine("\t,[last_change_datetime] = GetUtcDate()");

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
                    if (addedColumns.Any(col => col.ColumnName.ToLowerInvariant() == c.ColumnName.ToLowerInvariant()))
                        continue;

                    var quotedColumnName = new ObjectNameParser(c.ColumnName, "[", "]").FullQuotedString;
                    stringBuilder.AppendLine($"\t,{quotedColumnName} = [i].{quotedColumnName}");

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

                    var quotedColumnName = new ObjectNameParser(columnFilter.ColumnName, "[", "]").FullQuotedString;
                    stringBuilder.AppendLine($"\t,{quotedColumnName} = [i].{quotedColumnName}");

                    addedColumns.Add(columnFilter);
                }
            }


            stringBuilder.AppendLine($"FROM {trackingName.FullQuotedString} [side]");
            stringBuilder.Append($"JOIN INSERTED AS [i] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "[side]", "[i]"));
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"INSERT INTO {trackingName.FullQuotedString} (");

            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringPkAreNull = new StringBuilder();

            string argComma = string.Empty;
            string argAnd = string.Empty;
            foreach (var mutableColumn in this.tableDescription.PrimaryKey.Columns.Where(c => !c.IsReadOnly))
            {
                ObjectNameParser columnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilderArguments.AppendLine($"\t{argComma}[i].{columnName.FullQuotedString}");
                stringPkAreNull.Append($"{argAnd}[side].{columnName.FullQuotedString} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine("\t,[create_scope_id]");
            stringBuilder.AppendLine("\t,[create_timestamp]");
            stringBuilder.AppendLine("\t,[update_scope_id]");
            stringBuilder.AppendLine("\t,[update_timestamp]");
            stringBuilder.AppendLine("\t,[sync_row_is_tombstone]");
            stringBuilder.AppendLine("\t,[last_change_datetime]");


            //-----------------------------------------------------------
            // Adding the foreign keys
            //-----------------------------------------------------------
            addedColumns.Clear();
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

                    var quotedColumnName = new ObjectNameParser(c.ColumnName, "[", "]").FullQuotedString;
                    stringBuilder.AppendLine($"\t,{quotedColumnName}"); ;

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

                    var quotedColumnName = new ObjectNameParser(columnFilter.ColumnName, "[", "]").FullQuotedString;
                    stringBuilder.AppendLine($"\t,{quotedColumnName}"); ;

                    addedColumns.Add(columnFilter);
                }
            }


            stringBuilder.AppendLine(") ");
            stringBuilder.AppendLine("SELECT");
            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine("\t,NULL");
            stringBuilder.AppendLine("\t,@@DBTS+1");
            stringBuilder.AppendLine("\t,NULL");
            stringBuilder.AppendLine("\t,0");
            stringBuilder.AppendLine("\t,0");
            stringBuilder.AppendLine("\t,GetUtcDate()");


            //-----------------------------------------------------------
            // Adding the foreign keys
            //-----------------------------------------------------------
            addedColumns.Clear();

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

                    var quotedColumnName = new ObjectNameParser(c.ColumnName, "[", "]").FullQuotedString;
                    stringBuilder.AppendLine($"\t,[i].{quotedColumnName}"); ;

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

                    var quotedColumnName = new ObjectNameParser(columnFilter.ColumnName, "[", "]").FullQuotedString;
                    stringBuilder.AppendLine($"\t,[i].{quotedColumnName}"); ;

                    addedColumns.Add(columnFilter);
                }
            }


            stringBuilder.AppendLine("FROM INSERTED [i]");
            stringBuilder.Append($"LEFT JOIN {trackingName.FullQuotedString} [side] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "[i]", "[side]"));
            stringBuilder.Append("WHERE ");
            stringBuilder.AppendLine(stringPkAreNull.ToString());

            addedColumns.Clear();
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

                    var insTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger);
                    StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER {insTriggerName} ON {tableName.FullQuotedString} FOR INSERT AS");
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

                    var triggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger);

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
            var insTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger);
            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER {insTriggerName} ON {tableName.FullQuotedString} FOR INSERT AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            string str = $"Insert Trigger for table {tableName.FullQuotedString}";
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

                    var insTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger);
                    StringBuilder createTrigger = new StringBuilder($"ALTER TRIGGER {insTriggerName} ON {tableName.FullQuotedString} FOR INSERT AS ");
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
            var insTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger);
            StringBuilder createTrigger = new StringBuilder($"ALTER TRIGGER {insTriggerName} ON {tableName.FullQuotedString} FOR INSERT AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            string str = $"ALTER Trigger Insert for table {tableName.FullQuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }
        public string DropInsertTriggerScriptText()
        {
            var triggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger);
            var trigger = $"DELETE TRIGGER {triggerName};";
            var str = $"Drop Insert Trigger for table {tableName.FullQuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(trigger, str);
        }

        private string UpdateTriggerBodyText()
        {
            var addedColumns = new List<DmColumn>();
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("UPDATE [side] ");
            stringBuilder.AppendLine("SET \t[update_scope_id] = NULL -- since the update if from local, it's a NULL");
            stringBuilder.AppendLine("\t,[update_timestamp] = @@DBTS+1");
            stringBuilder.AppendLine("\t,[last_change_datetime] = GetUtcDate()");

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

                    var quotedColumnName = new ObjectNameParser(c.ColumnName, "[", "]").FullQuotedString;
                    stringBuilder.AppendLine($"\t,{quotedColumnName} = [i].{quotedColumnName}"); ;

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

                    var quotedColumnName = new ObjectNameParser(columnFilter.ColumnName, "[", "]").FullQuotedString;
                    stringBuilder.AppendLine($"\t,{quotedColumnName} = [i].{quotedColumnName}"); ;

                    addedColumns.Add(columnFilter);
                }
            }

            stringBuilder.AppendLine($"FROM {trackingName.FullQuotedString} [side]");
            stringBuilder.Append($"JOIN INSERTED AS [i] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "[side]", "[i]"));

            addedColumns.Clear();
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

                    var updTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger);
                    StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER {updTriggerName} ON {tableName.FullQuotedString} FOR UPDATE AS");
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

                    var triggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger);

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
            var updTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger);
            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER {updTriggerName} ON {tableName.FullQuotedString} FOR UPDATE AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.UpdateTriggerBodyText());

            string str = $"Update Trigger for table {tableName.FullQuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }
        public string DropUpdateTriggerScriptText()
        {
            var triggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger);
            var trigger = $"DELETE TRIGGER {triggerName};";
            var str = $"Drop Update Trigger for table {tableName.FullQuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(trigger, str);
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

                    var updTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger);
                    StringBuilder createTrigger = new StringBuilder($"ALTER TRIGGER {updTriggerName} ON {tableName.FullQuotedString} FOR UPDATE AS ");
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
            (var tableName, var trackingName) = SqlBuilder.GetParsers(this.tableDescription);
            var updTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger);

            StringBuilder createTrigger = new StringBuilder($"ALTER TRIGGER {updTriggerName} ON {tableName.FullQuotedString} FOR UPDATE AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            string str = $"ALTER Trigger Update for table {tableName.FullQuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }
        public bool NeedToCreateTrigger(DbTriggerType type)
        {

            var updTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger);
            var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger);
            var insTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger);

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
