using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Filter;
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
        private readonly ObjectNameParser tableName;
        private readonly ObjectNameParser trackingName;
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
            var addedColumns = new List<DmColumn>();

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("UPDATE [side] ");
            stringBuilder.AppendLine("SET \t[sync_row_is_tombstone] = 1");
            stringBuilder.AppendLine("\t,[update_scope_id] = NULL -- since the update if from local, it's a NULL");
            stringBuilder.AppendLine("\t,[update_timestamp] = @@DBTS+1");
            stringBuilder.AppendLine("\t,[last_change_datetime] = GetUtcDate()");

            stringBuilder.AppendLine($"FROM {this.trackingName.FullQuotedString} [side]");
            stringBuilder.Append($"JOIN DELETED AS [d] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "[side]", "[d]"));

            addedColumns.Clear();
            return stringBuilder.ToString();
        }
        public void CreateDeleteTrigger()
        {
            var alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger);


                    var createTrigger = new StringBuilder($"CREATE TRIGGER {delTriggerName} ON {this.tableName.FullQuotedString} FOR DELETE AS");
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
            var alreadyOpened = this.connection.State == ConnectionState.Open;

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
            var createTrigger = new StringBuilder($"CREATE TRIGGER {delTriggerName} ON {this.tableName.FullQuotedString} FOR DELETE AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.DeleteTriggerBodyText());

            var str = $"Delete Trigger for table {this.tableName.FullQuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }
        public void AlterDeleteTrigger()
        {
            var alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger);
                    var createTrigger = new StringBuilder($"ALTER TRIGGER {delTriggerName} ON {this.tableName.FullQuotedString} FOR DELETE AS ");
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
            var createTrigger = new StringBuilder($"ALTER TRIGGER {delTriggerName} ON {tableName.FullQuotedString} FOR DELETE AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            var str = $"ALTER Trigger Delete for table {tableName.FullQuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }
        public string DropDeleteTriggerScriptText()
        {
            var triggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger);
            var trigger = $"DELETE TRIGGER {triggerName};";
            var str = $"Drop Delete Trigger for table {this.tableName.FullQuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(trigger, str);
        }


        /// <summary>
        /// Check if tableDescription has one or more foreignkeys to parentTable 
        /// </summary>
        private bool IsForeignKeyTo(DmTable parentTable) =>
            this.IsForeignKeyTo(new[] { parentTable });

        /// <summary>
        /// Check if tableDescription has one or more foreignkeys to parentTable 
        /// </summary>
        private bool IsForeignKeyTo(DmTable childTable, DmTable parentTable) =>
            this.IsForeignKeyTo(childTable, new[] { parentTable });


        private bool IsForeignKeyTo(IEnumerable<DmTable> tables) => this.IsForeignKeyTo(this.tableDescription, tables);

        /// <summary>
        /// Check if tableDescription has one or more foreignkeys to list of tables
        /// </summary>
        private bool IsForeignKeyTo(DmTable childTable, IEnumerable<DmTable> tables)
        {
            if (childTable.ForeignKeys == null)
                return false;

            foreach (var table in tables)
            {
                foreach (var fk in childTable.ForeignKeys)
                {
                    if (fk.ParentTable.IsEqual(fk.ParentTable.TableName, table.TableName))
                    {
                        return true;
                    }
                }

            }

            return false;

        }

        /// <summary>
        /// Gets DmTable & DmColumn from a FilterClause2
        /// </summary>
        private (DmTable filterTable, DmColumn filterColumn) GetFilterDmColumn(FilterClause2 filter)
        {
            try
            {
                // get the column from the original filtered table (could be another table)
                var tableFilter = this.tableDescription.DmSet.Tables[filter.FilterTable.TableName.ObjectName];
                var columnFilter = tableFilter.Columns[filter.FilterTable.ColumnName];

                if (columnFilter == null)
                    throw new InvalidExpressionException($"Column {filter.FilterTable.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                return (tableFilter, columnFilter);
            }
            catch (Exception ex)
            {
                throw new InvalidExpressionException($"Can't get Filter column from {this.tableDescription.TableName}. Exception is {ex.Message}");
            }

        }

        /// <summary>
        /// Get all relations that are referenced as foreign keys and filtered columns, in a tracking table
        /// From a table to any filter table, get all DmRelation parents to this filter, with a defined depth
        /// </summary>
        /// <returns></returns>
        private List<DmRelation> GetFilteredRelations(DmTable table, int depth = 0)
        {
            var relations = new List<DmRelation>();

            foreach (var filter in this.Filters)
            {
                // if column is null, we are in a table that need a relation before
                if (string.IsNullOrEmpty(filter.FilterTable.ColumnName))
                    continue;

                // Gets filterTable and filterColumn from DmSet
                var (filterTable, filterColumn) = this.GetFilterDmColumn(filter);

                // get hierarchy from this table to filtertable
                var hierarchy = table.GetParentsTo(filterTable);

                depth = depth > 0 ? Math.Min(hierarchy.Count, depth) : hierarchy.Count;

                for (var i = 0; i < depth; i++)
                {
                    relations.Add(hierarchy[i]);
                }

            }

            return relations;

        }

        /// <summary>
        /// Get the update statement used in triggers
        /// </summary>
        private string GetUpdateStatement(ObjectNameParser trackingTable, bool isTombstone = false)
        {
            var stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"UPDATE [side] ");
            stringBuilder.AppendLine($"SET \t[sync_row_is_tombstone] = { (isTombstone ? "1" : "0") }");
            stringBuilder.AppendLine($"\t,[update_scope_id] = NULL -- Update locally");
            stringBuilder.AppendLine($"\t,[last_change_datetime] = GetUtcDate()");
            stringBuilder.Append($"FROM {trackingTable.FullQuotedString} [side]");

            return stringBuilder.ToString();

        }

        /// <summary>
        /// SELECT [down_N].[CustomerID], NULL, @@DBTS+1, NULL, 0, 0, GetUtcDate(), [up_N].[EmployeeId]
        /// We must go DOWN from tableDescription to dmTable, to get primary keys so [down_N]
        /// Then we must go up from dmTable to tableDescription to get filter column
        /// </summary>
        private string GetSelectInsertStatement(DmTable dmTable, DmTable tableDescription)
        {
            var downFormat = "[down_{0}]";
            var upFormat = "[up_{0}]";
            var addedColumns = new List<DmColumn>();
            var stringBuilder = new StringBuilder();
            (var dmTableName, var dmTrackingName) = SqlBuilder.GetParsers(dmTable);

            // result
            var stringBuilderArguments = new StringBuilder();

            var argComma = string.Empty;

            // Need to get the depth of the last table to get the primary keys
            var hierarchy = dmTable.GetParentsTo(tableDescription);

            // the primary keys needed in the select command comes from the last descendants
            // if no descendant, we can add them from INSERTED [i]
            var aliasDown = hierarchy.Count == 0 ? "[i]" : string.Format(downFormat, hierarchy.Count - 1);

            // Add mutable primary keys from the dmTable, in the insert statement 
            foreach (var mutableColumn in dmTable.PrimaryKey.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilderArguments.Append($"{argComma}{aliasDown}.{columnName.FullQuotedString}");
                argComma = ",";
                addedColumns.Add(mutableColumn);
            }

            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.Append(", NULL, @@DBTS+1, NULL, 0, 0, GetUtcDate()");

            // ---------------------------------------------------------------------
            // Add the filter columns if needed, and if not already added from Pkeys or Fkeys
            // ---------------------------------------------------------------------
            foreach (var filter in this.Filters)
            {
                // if column is null, we are in a table that need a relation before
                if (string.IsNullOrEmpty(filter.FilterTable.ColumnName))
                    continue;

                // get filter DmTable and DmColumn involved
                var filterTable = dmTable.DmSet.Tables[filter.FilterTable.TableName.ObjectNameNormalized];
                var filterColumn = filterTable.Columns[filter.FilterTable.ColumnName];

                if (filterColumn == null)
                    throw new InvalidExpressionException($"Column {filter.FilterTable.ColumnName} does not exist in Table {dmTable.TableName}");

                // get descendants if exist
                var hierarchy2 = this.tableDescription.GetParentsTo(filterTable);

                if (addedColumns.Any(ac => ac.ColumnName.ToLowerInvariant() == filterColumn.ColumnName.ToLowerInvariant()))
                    continue;

                var quotedColumnName = new ObjectNameParser(filterColumn.ColumnName, "[", "]").FullQuotedString;

                // alias 1 could be INSERTERD [i] if we are in level 0
                var aliasUp = hierarchy2.Count == 0 ? "[i]" : string.Format(upFormat, hierarchy2.Count - 1);

                stringBuilder.Append($", {aliasUp}.{quotedColumnName}"); ;

                addedColumns.Add(filterColumn);
            }
            stringBuilder.AppendLine();

            return stringBuilder.ToString();

        }

        private string GetInsertTriggerStatement(DmTable currentTable)
        {
            var stringBuilder = new StringBuilder();
            (var dmTableName, var dmTrackingName) = SqlBuilder.GetParsers(currentTable);

            stringBuilder.AppendLine("(");
            var argComma = string.Empty;
            // Add mutable primary keys in the insert statement
            foreach (var mutableColumn in currentTable.PrimaryKey.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilder.Append($"{argComma}{columnName.FullQuotedString}");
                argComma = ",";
            }

            stringBuilder.AppendLine();
            stringBuilder.Append(", [create_scope_id]");
            stringBuilder.Append(", [create_timestamp]");
            stringBuilder.Append(", [update_scope_id]");
            stringBuilder.Append(", [update_timestamp]");
            stringBuilder.Append(", [sync_row_is_tombstone]");
            stringBuilder.AppendLine(", [last_change_datetime]\t");

            // iterate through all columns that are foreign keys, from currentTable to Filter
            var dmColumns = this.GetFilteredRelations(currentTable).SelectMany(dr => dr.ParentColumns).ToList();

            foreach (var column in dmColumns)
            {
                var pt = new ObjectNameParser(column.Table.TableName).ObjectNameNormalized;
                var pc = new ObjectNameParser(column.ColumnName).ObjectNameNormalized;
                var ccc = new ObjectNameParser($"{pt}_{pc}").FullQuotedString;
                stringBuilder.Append($",{ccc} ");
            }
            stringBuilder.AppendLine(")");

            // Get the Select command 
            // Examples
            // Current table [CustomerAddress]. INSERTED [Employee]
            // SELECT [r1].[CustomerID], [r1].[AddressID]
            // Current table [CustomerAddress]. INSERTED [Customer]
            // SELECT [r0].[CustomerID], [r0].[AddressID]
            // Current table [CustomerAddress]. INSERTED [CustomerAddress]
            // SELECT [i].[CustomerID], [i].[AddressID]
            stringBuilder.Append("SELECT ");

            // get from tableDescription (INSERTED) DOWN to current table to get primary keys needed
            // if dmTable == tableDescription, no relations will be involved in
            var hierarchy = this.tableDescription.GetChildsTo(currentTable);

            // get the correct alias
            var alias = hierarchy.Count > 0 ? $"[r{hierarchy.Count - 1}]" : "[i]";

            foreach (var mutableColumn in currentTable.PrimaryKey.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilder.Append($"{alias}.{columnName.FullQuotedString}, ");
            }

            stringBuilder.Append(" NULL ,@@DBTS+1 ,NULL ,@@DBTS+1 ,0 ,GetUtcDate() ");

            stringBuilder.AppendLine();

            // Get From clause
            // Joined from INSERTED UP to filter
            // and from INSERTED DOWN to current table 
            stringBuilder.AppendLine("FROM INSERTED [i]");
            stringBuilder.AppendLine($"-- Get all rows INSERTED {this.tableName.FullQuotedString} UP TO Filter to get all columns needed to fill foreign keys");

            // Go from current table UP to filter
            var dmRelations = this.GetFilteredRelations(this.tableDescription);

            if (dmRelations.Count == 0)
                stringBuilder.AppendLine($"-- Not Needed here.");

            // needed later for joining the left clause on tracking table
            var leftJoinTrackingTableClauses = new StringBuilder();

            for (var indexRelation = 0; indexRelation < dmRelations.Count; indexRelation++)
            {
                var dmRelation = dmRelations[indexRelation];
                // get joined table
                var joinedDmTable = dmRelation.ParentTable;
                // get joined table name
                var joinedDmTableName = new ObjectNameParser(joinedDmTable.TableName);

                stringBuilder.Append($"INNER JOIN {joinedDmTableName.FullQuotedString} as [i{indexRelation}] ON ");

                var and = "";
                for (var indexColumn = 0; indexColumn < dmRelation.ChildColumns.Length; indexColumn++)
                {
                    var dmParentColumn = dmRelation.ParentColumns[indexColumn];
                    var dmParentColumnName = new ObjectNameParser(dmParentColumn.ColumnName).FullQuotedString;
                    var dmParentColumnAlias = $"[i{indexRelation}]";

                    var dmChildColumn = dmRelation.ChildColumns[indexColumn];
                    var dmChildColumnName = new ObjectNameParser(dmChildColumn.ColumnName).FullQuotedString;
                    var dmChildColumnAlias = indexRelation == 0 ? "[i]" : $"[i{indexRelation - 1}]";

                    stringBuilder.Append($"{and}{dmParentColumnAlias}.{dmParentColumnName} = {dmChildColumnAlias}.{dmChildColumnName}");

                    // needed later for left join clause
                    var pt = new ObjectNameParser(dmRelation.ChildTable.TableName).ObjectNameNormalized;
                    var pc = new ObjectNameParser(dmChildColumn.ColumnName).ObjectNameNormalized;
                    var ccc = new ObjectNameParser($"{pt}_{pc}").FullQuotedString;
                    leftJoinTrackingTableClauses.AppendLine($"and [side].{ccc} = {dmChildColumnAlias}.{dmChildColumnName}");

                    and = " AND ";
                }

                stringBuilder.AppendLine();
            }



            stringBuilder.AppendLine($"-- Get all rows from INSERTED {this.tableName.FullQuotedString} DOW TO current tracking table {dmTrackingName.FullQuotedString} to fill primary keys");

            dmRelations = this.tableDescription.GetChildsTo(currentTable);

            // needed later for joining the left clause on tracking table
            var leftJoinTrackingTableClauses2 = new StringBuilder();
            
            if (dmRelations.Count == 0)
            {
                stringBuilder.AppendLine($"-- Not Needed here.");

                // if nos left join to get clauses, we are at root level, so just make a left clause on pkeys
                var and = "";
                foreach (var pkey in this.tableDescription.PrimaryKey.Columns)
                {
                    var pkeyName = new ObjectNameParser(pkey.ColumnName).FullQuotedString;
                    leftJoinTrackingTableClauses2.Append($"{and}[side].{pkeyName} = [i].{pkeyName}");
                    and = " AND ";
                }
                stringBuilder.AppendLine();
            }


            for (var indexRelation = 0; indexRelation < dmRelations.Count; indexRelation++)
            {
                var dmRelation = dmRelations[indexRelation];
                // get joined table
                var joinedDmTable = dmRelation.ChildTable;
                // get joined table name
                var joinedDmTableName = new ObjectNameParser(joinedDmTable.TableName);

                stringBuilder.Append($"INNER JOIN {joinedDmTableName.FullQuotedString} AS [r{indexRelation}] ON ");

                var and = "";
                var and2 = dmRelations.Count == 0 ? "AND " : "";
                for (var indexColumn = 0; indexColumn < dmRelation.ChildColumns.Length; indexColumn++)
                {
                    var dmParentColumn = dmRelation.ParentColumns[indexColumn];
                    var dmParentColumnName = new ObjectNameParser(dmParentColumn.ColumnName).FullQuotedString;
                    var dmParentColumnAlias = indexRelation == 0 ? "[i]" : $"[r{indexRelation - 1}]";

                    var dmChildColumn = dmRelation.ChildColumns[indexColumn];
                    var dmChildColumnName = new ObjectNameParser(dmChildColumn.ColumnName).FullQuotedString;
                    var dmChildColumnAlias = $"[r{indexRelation}]";

                    stringBuilder.Append($"{and}{dmChildColumnAlias}.{dmChildColumnName} = {dmParentColumnAlias}.{dmParentColumnName} ");

                    // needed later for left join clause
                    var pt = new ObjectNameParser(dmRelation.ParentTable.TableName).ObjectNameNormalized;
                    var pc = new ObjectNameParser(dmParentColumn.ColumnName).ObjectNameNormalized;
                    var ccc = new ObjectNameParser($"{pt}_{pc}").FullQuotedString;

                    if (indexRelation == dmRelations.Count - 1)
                        ccc = dmParentColumnName;

                    leftJoinTrackingTableClauses2.AppendLine($"{and2}[side].{ccc} = {dmParentColumnAlias}.{dmParentColumnName}");

                    and = and2 = " AND ";
                }

                stringBuilder.AppendLine();
            }


            // Get Left join on current table to check if line did not already exists
            stringBuilder.AppendLine($"-- Make sure this line is not existing in the current [Customer_tracking] table ");
            stringBuilder.AppendLine($"-- Based on {dmTrackingName.FullQuotedString} foreign keys to ROOT FILTER.");

            stringBuilder.Append($"LEFT OUTER JOIN {dmTrackingName.FullQuotedString} [side] ON ");
            stringBuilder.Append(leftJoinTrackingTableClauses.ToString());
            stringBuilder.Append(leftJoinTrackingTableClauses2.ToString());
            stringBuilder.AppendLine("WHERE [side].[id] IS NULL");



            return stringBuilder.ToString();
        }

        private string GetInsertIntoStatement(DmTable dmTable)
        {
            var sideFilteredTable = "[base_{0}]";
            var addedColumns = new List<DmColumn>();
            var stringBuilder = new StringBuilder();
            (var dmTableName, var dmTrackingName) = SqlBuilder.GetParsers(dmTable);

            var stringBuilderArguments = new StringBuilder();

            var argComma = string.Empty;

            // Add mutable primary keys in the insert statement as well as in the where clause
            foreach (var mutableColumn in dmTable.PrimaryKey.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilderArguments.Append($"{argComma}{columnName.FullQuotedString}");
                argComma = ",";
            }

            stringBuilder.Append($"\t{stringBuilderArguments.ToString()}");
            stringBuilder.Append(", [create_scope_id]");
            stringBuilder.Append(", [create_timestamp]");
            stringBuilder.Append(", [update_scope_id]");
            stringBuilder.Append(", [update_timestamp]");
            stringBuilder.Append(", [sync_row_is_tombstone]");
            stringBuilder.AppendLine(", [last_change_datetime]\t");


            //-----------------------------------------------------------
            addedColumns.Clear();

            // Add primary keys columns to not add filter column that are already part of the primary keys
            foreach (var pkey in dmTable.PrimaryKey.Columns)
                addedColumns.Add(pkey);

            // ---------------------------------------------------------------------
            // Add the filter columns if needed, and if not already added from Pkeys or Fkeys
            // ---------------------------------------------------------------------
            foreach (var filter in this.Filters)
            {
                // if column is null, we are in a table that need a relation before
                if (string.IsNullOrEmpty(filter.FilterTable.ColumnName))
                    continue;

                var tableFilter = dmTable.DmSet.Tables[filter.FilterTable.TableName.ObjectNameNormalized];
                var columnFilter = tableFilter.Columns[filter.FilterTable.ColumnName];

                var hierarchy = dmTable.GetParentsTo(tableFilter);


                if (columnFilter == null)
                    throw new InvalidExpressionException($"Column {filter.FilterTable.ColumnName} does not exist in Table {dmTable.TableName}");

                if (addedColumns.Any(ac => ac.ColumnName.ToLowerInvariant() == columnFilter.ColumnName.ToLowerInvariant()))
                    continue;

                var quotedColumnName = new ObjectNameParser(columnFilter.ColumnName, "[", "]").FullQuotedString;

                var alias = hierarchy.Count == 0 ? "[i]" : string.Format(sideFilteredTable, $"{this.Filters.Count - 1}{ hierarchy.Count - 1}");

                stringBuilder.AppendLine($", {quotedColumnName}"); ;

                addedColumns.Add(columnFilter);
            }

            stringBuilder.Append(") ");

            return stringBuilder.ToString();
        }

        private string GetJoinClause(DmTable table1, DmTable table2, string alias1, string alias2)
        {
            var stringBuilder = new StringBuilder();

            var str = "";
            foreach (var dmRelation in table1.ChildRelations)
            {
                if (dmRelation.ChildTable.TableName.ToLowerInvariant() == table2.TableName.ToLowerInvariant())
                {
                    for (var i = 0; i < dmRelation.ChildColumns.Length; i++)
                    {
                        var col1 = dmRelation.ChildColumns[i];
                        var col2 = dmRelation.ParentColumns[i];

                        var quotedColumn1 = new ObjectNameParser(col1.ColumnName);
                        var quotedColumn2 = new ObjectNameParser(col2.ColumnName);

                        stringBuilder.Append(str);
                        stringBuilder.Append(alias1);
                        stringBuilder.Append(".");
                        stringBuilder.Append(quotedColumn1.FullQuotedString);
                        stringBuilder.Append(" = ");
                        stringBuilder.Append(alias2);
                        stringBuilder.Append(".");
                        stringBuilder.Append(quotedColumn2.FullQuotedString);

                        str = " AND ";

                    }
                }

            }

            return stringBuilder.ToString();

        }

        /// <summary>
        /// 2 examples here : 
        /// First   on CustomerAddress trigger
        /// Second  on Employee trigger
        /// 
        /// Tagert table : CustomerAddress
        /// 
        /// - Example on CustomerAddress in Employee trigger
        /// -- First part go Down from Employee (INSERTED) to CustomerAddress
        ///     INNER JOIN [Customer] [down_0] ON [down_0].[CustomerID] =  [i].[CustomerID] 
        ///     INNER JOIN [CustomerAddress] [down_1] ON [down_1].[CustomerID] = [down_1].[CustomerID]
        /// -- Second part go Up from Employee to Employee to get Employee Filter
        /// -- Not needed
        /// 
        /// - Example on CustomerAddress in CustomerAddress trigger
        /// -- First part go Down from CustomerAddress (INSERTED) to CustomerAddress
        /// -- Not needed
        /// -- Second part go Up from CustomerAddress to Employee to get Employee Filter
        ///     INNER JOIN [Customer] [up_0] ON [up_0].[CustomerID] =  [i].[CustomerID] 
        ///     INNER JOIN [Employee] [up_1] ON [up_1].[EmployeeID] =  [up_0].[CustomerID] 
        /// 
        /// Then add the check on new line
        /// LEFT JOIN  [CustomerAddress_tracking] [side] ON[i].[CustomerID] = [side].[CustomerID] AND[i].[EmployeeId] = [side].[EmployeeId]
        private string GetInsertJoinsStatement(DmTable dmTable)
        {
            var downFormat = "[down_{0}]";
            var upFormat = "[up_{0}]";

            var addedColumns = new List<DmColumn>();
            var stringBuilder = new StringBuilder();
            (var dmTableName, var dmTrackingName) = SqlBuilder.GetParsers(dmTable);

            (var whereClauseEqual, var whereClauseDifferent) = this.GetWhereClauses(dmTable, this.tableDescription, "[i]", "[d]");

            // Need to get the depth of the last table to get the primary keys
            // Ex 1 : From tableDescription (as INSERTED [i] == Employee) to DmTable (as CustomerAddress)
            // Ex 2 : From tableDescription (as INSERTED [i] == CustomerAdress) to DmTable CustomerAddress
            // 
            // Get all relations from TableDescription to dmTable.
            // if dmTable == tableDescription, no relations will be involved in
            var hierarchy = this.tableDescription.GetChildsTo(dmTable);

            // First Part
            // GO DOWN if needed
            // go from root to last descendant
            for (var index = 0; index < hierarchy.Count; index++)
            {
                // Examples
                // INNER JOIN [CHILD_0] [down_0] ON [i].[TABLEDESC_COL] = [down_0].[CHILD_0_FK]
                // INNER JOIN [CHILD_1] [down_1] ON [down_1].[CHILD_1_FK] =  [down_0].[CHILD_0_COL] 

                // alias 1 could be INSERTERD [i] if we are in level 0
                var alias1 = index == 0 ? "[i]" : string.Format(downFormat, (index - 1).ToString());
                // alias 2 represents the current table involved
                var alias2 = string.Format(downFormat, index);

                // get the current relation
                var relation = hierarchy[index];

                var join = this.GetJoinClause(relation.ParentTable, relation.ChildTable, alias1, alias2);

                var tableName = new ObjectNameParser(relation.ChildTable.TableName).FullQuotedString;
                join = $"INNER JOIN { tableName} AS {alias2} ON {join}";
                stringBuilder.AppendLine(join);
            }

            // GO UP THEN from dmTable to filterTable to be able to get the filter column

            // Need to get the depth of the last table to get the primary keys
            foreach (var filter in this.Filters)
            {
                if (string.IsNullOrEmpty(filter.FilterTable.ColumnName))
                    continue;

                // get filter DmTable and DmColumn involved
                var filterTable = dmTable.DmSet.Tables[filter.FilterTable.TableName.ObjectNameNormalized];
                var filterColumn = filterTable.Columns[filter.FilterTable.ColumnName];

                if (filterColumn == null)
                    throw new InvalidExpressionException($"Column {filter.FilterTable.ColumnName} does not exist in Table {dmTable.TableName}");

                // get descendants if exist
                var hierarchy2 = this.tableDescription.GetParentsTo(filterTable);

                for (var index = 0; index < hierarchy2.Count; index++)
                {
                    // Examples
                    //  INNER JOIN [Customer] [up_0] ON [up_0].[CustomerID] =  [i].[CustomerID] 
                    //  INNER JOIN [Employee] [up_1] ON [up_1].[EmployeeID] =  [up_0].[CustomerID] 

                    // alias 1 could be INSERTERD [i] if we are in level 0
                    var alias1 = index == 0 ? "[i]" : string.Format(upFormat, (index - 1).ToString());
                    // alias 2 represents the current table involved
                    var alias2 = string.Format(upFormat, index);

                    // get the current relation
                    var relation = hierarchy2[index];

                    var join = this.GetJoinClause(relation.ParentTable, relation.ChildTable, alias1, alias2);

                    var tableName = new ObjectNameParser(relation.ParentTable.TableName).FullQuotedString;
                    join = $"INNER JOIN { tableName} {alias2} ON {join}";
                    stringBuilder.AppendLine(join);
                }

            }


            // Create the last LEFT JOIN where we are sure this line does not exist
            // So make a join based on filter column
            // add all filters columns and join tracking table on it
            foreach (var c in dmTable.PrimaryKey.Columns)
            {
                if (this.tableDescription.Columns.Any(d => d.ColumnName.ToLowerInvariant() == c.ColumnName.ToLowerInvariant()))
                {
                    addedColumns.Add(c);
                }
            }

            foreach (var filter in this.Filters)
            {
                if (string.IsNullOrEmpty(filter.FilterTable.ColumnName))
                    continue;

                // get filter DmTable and DmColumn involved
                var filterColumn = dmTable.DmSet.Tables[filter.FilterTable.TableName.ObjectNameNormalized].Columns[filter.FilterTable.ColumnName];

                if (filterColumn == null)
                    throw new InvalidExpressionException($"Column {filter.FilterTable.ColumnName} does not exist in Table {dmTable.TableName}");

                // tableDescription represents the INSERTED [i] alias, and eventually represents the current tableDescription
                if (this.tableDescription.Columns.Any(d => d.ColumnName.ToLowerInvariant() == filterColumn.ColumnName.ToLowerInvariant()))
                {
                    if (addedColumns.Any(ac => ac.ColumnName.ToLowerInvariant() == filterColumn.ColumnName.ToLowerInvariant()))
                        continue;

                    addedColumns.Add(filterColumn);
                }

            }

            stringBuilder.Append($"LEFT JOIN {dmTrackingName.FullQuotedString} [side] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(addedColumns, "[i]", "[side]"));


            var str = stringBuilder.ToString();
            return str;
        }

        private (string whereEqual, string whereDifferent) GetWhereClauses(DmTable dmTable, DmTable tableDescription, string alias1, string alias2)
        {
            string whereClauseEqual = "", whereClauseDifferent = "", and = "";

            // Get all filtered tables from DmSet, based on name from Filters
            var filteredTables = this.Filters.Select(f => dmTable.DmSet.Tables[f.FilterTable.TableName.ObjectNameNormalized]);

            for (var filterIndex = 0; filterIndex < this.Filters.Count; filterIndex++)
            {
                var filter = this.Filters[filterIndex];

                if (string.IsNullOrEmpty(filter.FilterTable.ColumnName))
                    continue;

                // get filter DmTable
                var filterTable = filteredTables.Single(ft => ft.TableName.ToLowerInvariant() == filter.FilterTable.TableName.ObjectNameNormalized.ToLowerInvariant());
                var filterColumn = filterTable.Columns[filter.FilterTable.ColumnName];

                if (filterColumn == null)
                    throw new InvalidExpressionException($"Column {filter.FilterTable.ColumnName} does not exist in Table {dmTable.TableName}");

                var filterColumnName = new ObjectNameParser(filterColumn.ColumnName).FullQuotedString;

                // Add where clause
                if (tableDescription.Columns.Any(d => d.ColumnName.ToLowerInvariant() == filterColumn.ColumnName.ToLowerInvariant()))
                {
                    whereClauseEqual += $"{and} {alias1}.{filterColumnName} = {alias2}.{filterColumnName} ";
                    whereClauseDifferent += $"{and} {alias1}.{filterColumnName} IS NULL ";
                    and = "AND";
                }

            }

            return (whereClauseEqual, whereClauseDifferent);
        }

        /// <summary>
        /// Get all statements
        /// </summary>
        /// <returns></returns>
        private (string innerJoinInserted, string innerJoinDeleted, string leftJoinInserted, string leftJoinDeleted)
            GetJoinsAndWhereClauses(DmTable dmTable, DmTable tableDescription)
        {
            var addedColumns = new List<DmColumn>();
            string innerJoinInserted = "", leftJoinInserted = "", innerJoinDeleted = "", leftJoinDeleted = "";

            var primaryKeysColumnsAndFilteredColumns = new List<DmColumn>();

            foreach (var pkey in dmTable.PrimaryKey.Columns)
            {
                if (tableDescription.Columns.Any(d => d.ColumnName.ToLowerInvariant() == pkey.ColumnName.ToLowerInvariant()))
                {
                    primaryKeysColumnsAndFilteredColumns.Add(pkey);
                    addedColumns.Add(pkey);
                }
            }

            // Get all filtered tables from DmSet, based on name from Filters
            var filteredTables = this.Filters.Select(f => dmTable.DmSet.Tables[f.FilterTable.TableName.ObjectNameNormalized]);


            // Check if dmTable is a foreign table to one of the filters
            var isAForeignTableToFilter = this.IsForeignKeyTo(filteredTables);

            for (var filterIndex = 0; filterIndex < this.Filters.Count; filterIndex++)
            {
                var filter = this.Filters[filterIndex];

                if (string.IsNullOrEmpty(filter.FilterTable.ColumnName))
                    continue;

                // get filter DmTable
                var filterTable = filteredTables.Single(ft => ft.TableName.ToLowerInvariant() == filter.FilterTable.TableName.ObjectNameNormalized.ToLowerInvariant());
                var filterColumn = filterTable.Columns[filter.FilterTable.ColumnName];

                if (filterColumn == null)
                    throw new InvalidExpressionException($"Column {filter.FilterTable.ColumnName} does not exist in Table {dmTable.TableName}");

                var filterColumnName = new ObjectNameParser(filterColumn.ColumnName).FullQuotedString;

                // don't add the column to inner join clause if already present in primary keys columns
                if (addedColumns.Any(ac => ac.ColumnName.ToLowerInvariant() == filterColumn.ColumnName.ToLowerInvariant()))
                    continue;

                if (!tableDescription.Columns.Any(d => d.ColumnName.ToLowerInvariant() == filterColumn.ColumnName.ToLowerInvariant()))
                    continue;


                // add columns for inner join clause
                primaryKeysColumnsAndFilteredColumns.Add(filterColumn);
            }

            innerJoinInserted = $"INNER JOIN INSERTED AS [i] ON {SqlManagementUtils.JoinTwoTablesOnClause(primaryKeysColumnsAndFilteredColumns, "[side]", "[i]")}";
            leftJoinInserted = $"LEFT OUTER JOIN INSERTED AS [i] ON {SqlManagementUtils.JoinTwoTablesOnClause(primaryKeysColumnsAndFilteredColumns, "[side]", "[i]")}";
            innerJoinDeleted = $"INNER JOIN DELETED AS [d] ON {SqlManagementUtils.JoinTwoTablesOnClause(primaryKeysColumnsAndFilteredColumns, "[side]", "[d]")}";
            leftJoinDeleted = $"LEFT OUTER JOIN DELETED AS [d] ON {SqlManagementUtils.JoinTwoTablesOnClause(primaryKeysColumnsAndFilteredColumns, "[side]", "[d]")}";

            return (innerJoinInserted, innerJoinDeleted, leftJoinInserted, leftJoinDeleted);

        }

        /// <summary>
        /// UPDATE TRIGGER : Get all columns that should be part of a Join clause
        /// In this example, tableDescription is Employee :
        /// INNER JOIN [DELETED] AS[d] ON[d].[EmployeeID] = [side].[Employee_EmployeeID] and[d].[RegionID] = [side].[Region_RegionID]
        /// DmColumns are EmployeeID (Pkey) and RegionId (Fkey) that are pkey / fkey AND part of the filter
        /// </summary>
        private List<DmColumn> GetFilteredJoinedColumns()
        {
            // Get primary keys and foreign keys, part of filter, from tableDescription
            var tableDescPrimaryKeysColumns = this.tableDescription.PrimaryKey.Columns;

            // get foreign keys to table parent, which is part of the filter
            var tbAllKeysColumns = this.GetFilteredRelations(this.tableDescription, 1).SelectMany(dr => dr.ChildColumns).ToList();

            // exclusive union of previous
            tbAllKeysColumns.AddRange(tableDescPrimaryKeysColumns.Where(k => !tbAllKeysColumns.Any(fk => fk.ColumnName.ToLowerInvariant() == k.ColumnName.ToLowerInvariant())));

            return tbAllKeysColumns;
        }


        /// <summary>
        /// UPDATE TRIGGER : Get the Where clause
        /// Can be equal or different
        /// Example. Equality :
        ///  ([i].[RegionID] = [d].[RegionID] and [i].[EmployeeID] = [d].[EmployeeID])
        /// Example. Different:
        ///  ([i].[RegionID] <> [d].[RegionID] or [i].[EmployeeID] <> [d].[EmployeeID]) OR ([i].[RegionID] is null and [i].[EmployeeID] is null)
        /// </summary>
        private string GetUpdateTriggerWhereClauses(DmTable currentTable, bool shouldBeDifferent)
        {
            // from tableDescription (current INSERTED / DELETED) get all columns involved in fkey / pkey to be checked
            var columns = this.GetFilteredJoinedColumns();

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"-- Check if the foreign keys in [INSERTED] / [DELETED] from {this.tableName.FullQuotedString}, and part of the filter, have not changed.");

            var comparer = shouldBeDifferent ? "<>" : "=";
            var and = "";

            stringBuilder.Append("(");

            // Equality
            foreach (var column in columns)
            {
                var columnName = new ObjectNameParser(column.ColumnName);

                stringBuilder.Append($"{and}[i].{columnName.FullQuotedString} {comparer} [d].{columnName.FullQuotedString}");
                and = " AND ";
            }
            stringBuilder.Append(")");

            // Adding Difference if needed
            if (shouldBeDifferent)
            {
                stringBuilder.Append(" OR (");

                and = "";
                foreach (var column in columns)
                {
                    var columnName = new ObjectNameParser(column.ColumnName);

                    stringBuilder.Append($"{and}[i].{columnName.FullQuotedString} IS NULL");
                    and = " AND ";
                }
                stringBuilder.Append(")");

            }

            return stringBuilder.ToString();
        }


        /// <summary>
        /// UPDATE TRIGGER : Get join clause for UPDATE [side] trigger. [side] the current table
        /// INSERTED and DELETED are the tableDescription
        /// In this example, tableDescription is Employee and filter is [Region]:
        /// INNER JOIN [DELETED] AS[d] ON[d].[EmployeeID] = [side].[Employee_EmployeeID] and[d].[RegionID] = [side].[Region_RegionID]
        /// </summary>
        /// <param name="currentTable">Can be Root Filter [Region] to any table to Leaf table [CustomerAddress]</param>
        /// <returns></returns>
        private string GetUpdateTriggerJoinClauses(DmTable currentTable, string innerText1, string innerText2)
        {
            (var currenTableName, var currentTrackingTableName) = SqlBuilder.GetParsers(currentTable);

            var sb = new StringBuilder();
            sb.AppendLine($"-- Get all {currentTrackingTableName} rows joined from [INSERTED] and [DELETED] based " +
                          $"on primary keys and foreign keys (if exists) from {this.tableName}.");


            // Get foreign keys in currenTable, who are part of the filter
            var sideFilteredRelations = this.GetFilteredRelations(currentTable);

            var deletedString = new StringBuilder($"{innerText1} JOIN DELETED [d] on ");
            var insertedString = new StringBuilder($"{innerText2} JOIN INSERTED [i] on ");
            var and = "";

            // Get all columns that should be use in the join clauses
            var tbAllKeysColumns = this.GetFilteredJoinedColumns();

            // iterate through all column that are PKEY or FKEY in tableDescription (which are available through INSERTED and DELETED)
            // 1st case : column is a primary key and currentTable IS tableDescription
            // -- [d] ON [d].[PKEY] = [side].[PKEY]
            //
            // 2nd case : column is a primary key and currentTable IS NOT tableDescription  and we found this in the sideFiltereredRelations
            // -- [d] ON [d].[PKEY] = [side].[PKEY]
            //
            // 3nd case : column is a foreign key and we found this in the sideFiltereredRelations
            // -- [d] ON [d].[FKEY] = [side].[TABLEFKEY_FKEY]
            //
            foreach (var column in tbAllKeysColumns)
            {
                // get corresponding column in sideFilteredColumns
                var tableDescColumnName = new ObjectNameParser(column.ColumnName).FullQuotedString;

                // check if we have a primary key
                var isPrimaryKey = this.tableDescription.PrimaryKey.Columns.Any(c => c.ColumnName.ToLowerInvariant() == column.ColumnName.ToLowerInvariant() && c.Table.TableName.ToLowerInvariant() == column.Table.TableName.ToLowerInvariant());

                // if we are at the root of trigger (ie tableDescription == currentTable)
                var isRoot = this.tableDescription.TableName.ToLowerInvariant() == currentTable.TableName.ToLowerInvariant();

                // 1st Case : Primary key and && root
                if (isPrimaryKey && isRoot)
                {
                    deletedString.Append($"{and}[d].{tableDescColumnName} = [side].{tableDescColumnName} ");
                    insertedString.Append($"{and}[i].{tableDescColumnName} = [side].{tableDescColumnName} ");
                    continue;
                }

                // 2nd Case : Primary Keys but not on root
                if (isPrimaryKey && !isRoot)
                {
                    // search if we have this column in sideFilteredRelations
                    // Since we have a Primary key, we are searching for the ParentTable where
                    // so search for a DmRelation where Parent Table IS the column table we are currently iterate
                    var cc = sideFilteredRelations
                            .Where(dm => dm.ParentTable.TableName.ToLowerInvariant() == column.Table.TableName.ToLowerInvariant())
                            .Select(dm => dm.ParentColumns.Single(pc => pc.ColumnName.ToLowerInvariant() == column.ColumnName.ToLowerInvariant()))
                            .FirstOrDefault();

                    // then get the parent relation to get parent table and parent column
                    if (cc != null)
                    {
                        var pt = new ObjectNameParser(cc.Table.TableName).ObjectNameNormalized;
                        var pc = new ObjectNameParser(cc.ColumnName).ObjectNameNormalized;
                        var ccc = new ObjectNameParser($"{pt}_{pc}").FullQuotedString;
                        deletedString.Append($"{and}[d].{tableDescColumnName} = [side].{ccc} ");
                        insertedString.Append($"{and}[i].{tableDescColumnName} = [side].{ccc} ");
                        and = "AND ";
                    }
                    continue;
                }
                // 3nd case : not a primary key and not on root
                if (!isPrimaryKey)
                {
                    // search if we have this column in sideFilteredRelations
                    // so search for a DmRelation where ChildTable IS the column table we are currently iterate
                    var cc = sideFilteredRelations
                            .Where(dm => dm.ChildTable.TableName.ToLowerInvariant() == column.Table.TableName.ToLowerInvariant())
                            .Select(dm => dm.ParentColumns.Single(pc => pc.ColumnName.ToLowerInvariant() == column.ColumnName.ToLowerInvariant()))
                            .FirstOrDefault();

                    // then get the parent relation to get parent table and parent column
                    if (cc != null)
                    {
                        var pt = new ObjectNameParser(cc.Table.TableName).ObjectNameNormalized;
                        var pc = new ObjectNameParser(cc.ColumnName).ObjectNameNormalized;
                        var ccc = new ObjectNameParser($"{pt}_{pc}").FullQuotedString;
                        deletedString.Append($"{and}[d].{tableDescColumnName} = [side].{ccc} ");
                        insertedString.Append($"{and}[i].{tableDescColumnName} = [side].{ccc} ");
                        and = "AND ";
                    }
                }

                and = "AND ";
            }


            sb.AppendLine(deletedString.ToString());
            sb.Append(insertedString.ToString());

            var str = sb.ToString();
            return str;
        }

        /// <summary>
        /// Create insert trigger text statement
        /// </summary>
        private string InsertTriggerBodyText()
        {
            var addedColumns = new List<DmColumn>();
            var primaryKeysColumnsAndFilteredColumns = this.tableDescription.PrimaryKey.Columns.ToList();

            // Get all filtered tables from DmSet, based on name from Filters
            var filteredTables = this.Filters.Select(f => this.tableDescription.DmSet.Tables[f.FilterTable.TableName.ObjectNameNormalized]);

            // Check if tabledescription is a foreign table to one of the filters OR if tableDescription IS the filtered table
            var isAForeignTableToFilter = this.IsForeignKeyTo(filteredTables);

            // check if the Tabledescription IS a filter itself
            var isRootTableFiltered = filteredTables.Any(f => f.TableName.ToLowerInvariant() == this.tableDescription.TableName.ToLowerInvariant());

            // get all joins
            (var innerJoinInserted, var innerJoinDeleted, var leftJoinInserted, var leftJoinDeleted) = this.GetJoinsAndWhereClauses(this.tableDescription, this.tableDescription);
            (var whereClauseEqual, var whereClauseDifferent) = this.GetWhereClauses(this.tableDescription, this.tableDescription, "[i]", "[d]");

            var stringBuilder = new StringBuilder();

            // inner join on deleted to check if filter has changed
            if (isAForeignTableToFilter || isRootTableFiltered)
            {
                void CreateUpdateTombstoneRow(DmTable currentTable)
                {
                    // for current table, get tracking and table names
                    (var tb, var tk) = SqlBuilder.GetParsers(currentTable);

                    stringBuilder.AppendLine();
                    stringBuilder.AppendLine($"-------------------------------------");
                    stringBuilder.AppendLine($"-- {tb.FullQuotedString} --");
                    stringBuilder.AppendLine($"-------------------------------------");

                    // First update is only when filtered columns are not impacted  
                    stringBuilder.AppendLine();
                    stringBuilder.AppendLine($"-- Update rows into {tb.FullQuotedString} when primary keys and foreign keys, part of a filter, have not changed.");
                    stringBuilder.AppendLine(this.GetUpdateStatement(tk, false));
                    stringBuilder.AppendLine(this.GetUpdateTriggerJoinClauses(currentTable, "INNER", "INNER"));
                    stringBuilder.Append($"WHERE ");
                    stringBuilder.AppendLine(this.GetUpdateTriggerWhereClauses(currentTable, false));

                    // Second update: Only when filtered columns are impacted. Mark old rows as Tombstone 
                    stringBuilder.AppendLine();
                    stringBuilder.AppendLine($"-- Mark rows as Tombstone on {tb.FullQuotedString} when primary keys and foreign keys, part of a filter, have not changed.");
                    stringBuilder.AppendLine("");
                    stringBuilder.AppendLine(this.GetUpdateStatement(tk, true));
                    stringBuilder.AppendLine(this.GetUpdateTriggerJoinClauses(currentTable, "INNER", "LEFT OUTER"));
                    stringBuilder.Append("WHERE ");
                    stringBuilder.AppendLine(this.GetUpdateTriggerWhereClauses(currentTable, true));

                    stringBuilder.AppendLine();
                    stringBuilder.AppendLine($"INSERT INTO {tk.FullQuotedString}");
                    stringBuilder.AppendLine(this.GetInsertTriggerStatement(currentTable));
                    // Get all child tables that are foreign tkeys to this table
                    var oneLevelTables = currentTable.DmSet.Tables.Where(dt => this.IsForeignKeyTo(dt, currentTable));

                    foreach (var dmTable in oneLevelTables)
                    {
                        CreateUpdateTombstoneRow(dmTable);
                    }

                }

                CreateUpdateTombstoneRow(this.tableDescription);
            }
            else
            {
                innerJoinInserted = $"INNER JOIN INSERTED AS [i] ON {SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "[side]", "[i]")}";
                innerJoinDeleted = $"INNER JOIN DELETED AS [d] ON {SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "[side]", "[d]")}";

                // First update is only when filtered columns are not impacted  
                stringBuilder.AppendLine();
                stringBuilder.AppendLine("-- Update line.Check filters");
                stringBuilder.AppendLine("-- This update is executed if a filter existes and has not changed at all");
                stringBuilder.AppendLine("-- If the filter has changed the JOIN INSERTED won't work and return no rows");
                stringBuilder.AppendLine(this.GetUpdateStatement(this.trackingName, false));
                stringBuilder.AppendLine(innerJoinInserted);

            }


            stringBuilder.AppendLine();
            stringBuilder.AppendLine();


            void CreateInsertIntoCommand(DmTable currentTable)
            {
                (var tb, var tk) = SqlBuilder.GetParsers(currentTable);

                (var t_whereClauseEqual, var t_whereClauseDifferent) = this.GetWhereClauses(currentTable, this.tableDescription, "[side]", "[d]");

                stringBuilder.AppendLine();
                stringBuilder.AppendLine("----------------------------------------------------");
                stringBuilder.AppendLine($"--Mark the new row on {tk.FullQuotedString} as new when filter has changed");
                stringBuilder.AppendLine("----------------------------------------------------");
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"INSERT INTO {tk.FullQuotedString} (");

                // Add insert statement
                stringBuilder.AppendLine(this.GetInsertIntoStatement(currentTable));

                stringBuilder.Append("SELECT ");

                // Add Select values to insert
                stringBuilder.Append(this.GetSelectInsertStatement(currentTable, this.tableDescription));

                stringBuilder.AppendLine($"FROM [INSERTED] [i]");

                stringBuilder.Append(this.GetInsertJoinsStatement(currentTable));

                stringBuilder.Append("WHERE ");

                var argAnd = "";
                foreach (var mutableColumn in currentTable.PrimaryKey.Columns.Where(c => !c.IsReadOnly))
                {
                    var columnName = new ObjectNameParser(mutableColumn.ColumnName);
                    stringBuilder.Append($"{argAnd}[side].{columnName.FullQuotedString} IS NULL");
                    argAnd = " AND ";
                }

                if (!string.IsNullOrEmpty(t_whereClauseDifferent))
                    stringBuilder.AppendLine($"{argAnd} {t_whereClauseDifferent}");

                // Get all child tables that are foreign tkeys to this table
                var oneLevelTables = currentTable.DmSet.Tables.Where(dt => this.IsForeignKeyTo(dt, currentTable));

                foreach (var dmTable in oneLevelTables)
                {
                    CreateInsertIntoCommand(dmTable);
                }

            }

            //CreateInsertIntoCommand(this.tableDescription);

            addedColumns.Clear();
            var str = stringBuilder.ToString();
            return str;
        }
        public void CreateInsertTrigger()
        {
            var alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var insTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger);
                    var createTrigger = new StringBuilder($"CREATE TRIGGER {insTriggerName} ON {this.tableName.FullQuotedString} FOR INSERT AS");
                    createTrigger.AppendLine();
                    createTrigger.AppendLine(this.InsertTriggerBodyText());

                    command.CommandText = createTrigger.ToString();
                    command.Connection = this.connection;

                    // TODO : DISABLE EXECUTE QUERY ON CREATE INSERT TRIGGER
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
            var alreadyOpened = this.connection.State == ConnectionState.Open;

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
            var createTrigger = new StringBuilder($"CREATE TRIGGER {insTriggerName} ON {this.tableName.FullQuotedString} FOR INSERT AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            var str = $"Insert Trigger for table {this.tableName.FullQuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);

        }
        public void AlterInsertTrigger()
        {
            var alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var insTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger);
                    var createTrigger = new StringBuilder($"ALTER TRIGGER {insTriggerName} ON {this.tableName.FullQuotedString} FOR INSERT AS ");
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
            var createTrigger = new StringBuilder($"ALTER TRIGGER {insTriggerName} ON {this.tableName.FullQuotedString} FOR INSERT AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            var str = $"ALTER Trigger Insert for table {this.tableName.FullQuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }
        public string DropInsertTriggerScriptText()
        {
            var triggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger);
            var trigger = $"DELETE TRIGGER {triggerName};";
            var str = $"Drop Insert Trigger for table {this.tableName.FullQuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(trigger, str);
        }

        private string UpdateTriggerBodyText()
        {
            return this.InsertTriggerBodyText();

            var addedColumns = new List<DmColumn>();
            var stringBuilder = new StringBuilder();
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

            stringBuilder.AppendLine($"FROM {this.trackingName.FullQuotedString} [side]");
            stringBuilder.Append($"JOIN INSERTED AS [i] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "[side]", "[i]"));

            addedColumns.Clear();
            return stringBuilder.ToString();
        }
        public void CreateUpdateTrigger()
        {
            // TODO : DEV TIME : DISABLE CREATE UPDATE TRIGGER;

            return;
            var alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var updTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger);
                    var createTrigger = new StringBuilder($"CREATE TRIGGER {updTriggerName} ON {this.tableName.FullQuotedString} FOR UPDATE AS");
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
            var alreadyOpened = this.connection.State == ConnectionState.Open;

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
            var createTrigger = new StringBuilder($"CREATE TRIGGER {updTriggerName} ON {this.tableName.FullQuotedString} FOR UPDATE AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.UpdateTriggerBodyText());

            var str = $"Update Trigger for table {this.tableName.FullQuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }
        public string DropUpdateTriggerScriptText()
        {
            var triggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger);
            var trigger = $"DELETE TRIGGER {triggerName};";
            var str = $"Drop Update Trigger for table {this.tableName.FullQuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(trigger, str);
        }
        public void AlterUpdateTrigger()
        {
            var alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var updTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger);
                    var createTrigger = new StringBuilder($"ALTER TRIGGER {updTriggerName} ON {this.tableName.FullQuotedString} FOR UPDATE AS ");
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

            var createTrigger = new StringBuilder($"ALTER TRIGGER {updTriggerName} ON {tableName.FullQuotedString} FOR UPDATE AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            var str = $"ALTER Trigger Update for table {tableName.FullQuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }
        public bool NeedToCreateTrigger(DbTriggerType type)
        {

            var updTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger);
            var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger);
            var insTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger);

            var triggerName = string.Empty;
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

            return !SqlManagementUtils.TriggerExists(this.connection, this.transaction, triggerName);


        }


    }
}
