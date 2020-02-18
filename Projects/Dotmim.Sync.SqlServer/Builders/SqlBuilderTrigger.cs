using Dotmim.Sync.Builders;


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
        private ParserName tableName;
        private ParserName trackingName;
        private readonly SyncTable tableDescription;
        private readonly SqlConnection connection;
        private readonly SqlTransaction transaction;
        private readonly SqlObjectNames sqlObjectNames;
        private readonly SqlDbMetadata sqlDbMetadata;

        public SqlBuilderTrigger(SyncTable tableDescription, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as SqlConnection;
            this.transaction = transaction as SqlTransaction;

            this.tableDescription = tableDescription;
            (this.tableName, this.trackingName) = SqlTableBuilder.GetParsers(this.tableDescription);
            this.sqlObjectNames = new SqlObjectNames(this.tableDescription);
            this.sqlDbMetadata = new SqlDbMetadata();

        }

        private string DeleteTriggerBodyText()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("SET NOCOUNT ON;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("UPDATE [side] ");
            stringBuilder.AppendLine("SET  [sync_row_is_tombstone] = 1");
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
            stringBuilder.AppendLine("\t,1");
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


        private string InsertTriggerBodyText()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("SET NOCOUNT ON;");
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


        private List<(SyncTable Table, SyncColumn Column)> GetFilteredColumns(SyncFilter filter)
        {
            if (filter == null)
                return null;

            var cols = new List<(SyncTable, SyncColumn)>();


            foreach (var p in filter.Parameters)
            {
                // Get where for this parameter
                var where = filter.Wheres.FirstOrDefault(w => w.ParameterName == p.Name);

                if (where == null)
                    continue;
                var table = this.tableDescription.Schema.Tables[where.TableName, where.SchemaName];
                var col = table.Columns[where.ColumnName];

                cols.Add((table, col));
            }

            return cols;

        }

        private string GetDeclarationColumn(SyncColumn column)
        {

            var quotedColumnName = ParserName.Parse(column).Quoted().ToString();

            var columnTypeString = this.sqlDbMetadata.TryGetOwnerDbTypeString(column.OriginalDbType, column.GetDbType(), false, false, column.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
            var quotedColumnType = ParserName.Parse(columnTypeString).Quoted().ToString();
            var columnPrecisionString = this.sqlDbMetadata.TryGetOwnerDbTypePrecision(column.OriginalDbType, column.GetDbType(), false, false, column.MaxLength, column.Precision, column.Scale, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
            var columnType = $"{quotedColumnType} {columnPrecisionString}";

            return $"{quotedColumnName} {columnType}, ";
        }

        /// <summary>
        /// Get a list of columns to add in the @LINES temp table
        /// </summary>
        private string GetListLinesColumns(SyncTable table, string alias)
        {
            var primaryKeys = table.GetPrimaryKeysColumns();
            var filter = table.GetFilter();
            var stringBuilder = new StringBuilder();
            var filterColumnsAndTables = this.GetFilteredColumns(filter);

            foreach (var pk in primaryKeys)
                stringBuilder.Append($"[{alias}].{ParserName.Parse(pk).Quoted().ToString()}, ");

            // Add filtered columns
            if (filter != null)
            {
                foreach (var fct in filterColumnsAndTables)
                {
                    // do not add this column if it's a primary key column and already added
                    if (primaryKeys.Any(pk => pk == fct.Column))
                        continue;

                    string filterTableName = ParserName.Parse(fct.Table).Quoted().ToString();
                    string filterColumnName = ParserName.Parse(fct.Column).Quoted().ToString();

                    if (fct.Table == this.tableDescription)
                        filterTableName = $"[{alias}]";

                    stringBuilder.Append($"{filterTableName}.{filterColumnName},");
                }
            }

            return stringBuilder.ToString();
        }


        private string GetInnerJoins(SyncTable table, string alias)
        {
            var filter = table.GetFilter();

            if (filter == null)
                return string.Empty;

            var stringBuilder = new StringBuilder();

            foreach (var customJoin in filter.Joins)
            {
                // Get current table to compare if we need to replace for [I] or [D]
                var fullTableName = string.IsNullOrEmpty(table.SchemaName) ? table.TableName : $"{table.SchemaName}.{table.TableName}";
                var filterTableName = ParserName.Parse(fullTableName).Quoted().Schema().ToString();

                var joinTableName = ParserName.Parse(customJoin.TableName).Quoted().Schema().ToString();

                var leftTableName = ParserName.Parse(customJoin.LeftTableName).Quoted().Schema().ToString();
                if (string.Equals(filterTableName, leftTableName, SyncGlobalization.DataSourceStringComparison))
                    leftTableName = $"[{alias}]";

                var rightTableName = ParserName.Parse(customJoin.RightTableName).Quoted().Schema().ToString();
                if (string.Equals(filterTableName, rightTableName, SyncGlobalization.DataSourceStringComparison))
                    rightTableName = $"[{alias}]";

                var leftColumName = ParserName.Parse(customJoin.LeftColumnName).Quoted().ToString();
                var rightColumName = ParserName.Parse(customJoin.RightColumnName).Quoted().ToString();

                stringBuilder.AppendLine($"INNER JOIN {joinTableName} ON {leftTableName}.{leftColumName} = {rightTableName}.{rightColumName}");

            }

            return stringBuilder.ToString();
        }

        private string GetReverseInnerJoins(SyncTable table, string alias)
        {
            var filter = table.GetFilter();

            if (filter == null)
                return string.Empty;

            var stringBuilder = new StringBuilder();


            

            foreach (var customJoin in filter.Joins)
            {
                // Get current table to compare if we need to replace for [I] or [D]
                var fullTableName = string.IsNullOrEmpty(table.SchemaName) ? table.TableName : $"{table.SchemaName}.{table.TableName}";
                var filterTableName = ParserName.Parse(fullTableName).Quoted().Schema().ToString();

                var joinTableName = ParserName.Parse(customJoin.TableName).Quoted().Schema().ToString();

                var leftTableName = ParserName.Parse(customJoin.LeftTableName).Quoted().Schema().ToString();
                if (string.Equals(filterTableName, leftTableName, SyncGlobalization.DataSourceStringComparison))
                    leftTableName = $"[{alias}]";

                var rightTableName = ParserName.Parse(customJoin.RightTableName).Quoted().Schema().ToString();
                if (string.Equals(filterTableName, rightTableName, SyncGlobalization.DataSourceStringComparison))
                    rightTableName = $"[{alias}]";

                var leftColumName = ParserName.Parse(customJoin.LeftColumnName).Quoted().ToString();
                var rightColumName = ParserName.Parse(customJoin.RightColumnName).Quoted().ToString();

                stringBuilder.AppendLine($"INNER JOIN {joinTableName} ON {leftTableName}.{leftColumName} = {rightTableName}.{rightColumName}");

            }

            return stringBuilder.ToString();
        }


        /// <summary>
        /// Get all column for LINES that represent the primary key for tracking table
        /// </summary>
        private List<SyncColumn> GetPrimaryKeysForLinesTable(SyncTable table)
        {
            var primaryKeys = table.GetPrimaryKeysColumns();
            var filter = table.GetFilter();

            if (filter == null)
                return primaryKeys.ToList();

            var filterColumnsAndTables = this.GetFilteredColumns(filter);
            var lst = new List<SyncColumn>();

            // Add primary keys as part of the @lines table
            foreach (var pkColumn in primaryKeys)
                lst.Add(pkColumn);

            // Add filtered columns
            foreach (var fct in filterColumnsAndTables)
            {
                // do not add this column if it's a primary key column and already added
                if (primaryKeys.Any(pk => pk == fct.Column))
                    continue;

                lst.Add(fct.Column);
            }

            return lst;
        }

        private string CreateLinesTableDeclaration()
        {
            var stringBuilder = new StringBuilder();
            var primaryKeys = this.tableDescription.GetPrimaryKeysColumns();

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("SET NOCOUNT ON;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("DECLARE @LINES TABLE (");

            var lstPkeys = GetPrimaryKeysForLinesTable(this.tableDescription);

            // Add primary keys as part of the @lines table
            foreach (var pkColumn in lstPkeys)
                stringBuilder.Append(GetDeclarationColumn(pkColumn));

            // Add metadata rows
            stringBuilder.AppendLine("[sync_row_is_tombstone] [bit], [Level] [tinyint]);");

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("INSERT INTO @LINES");
            stringBuilder.Append("SELECT ");
            stringBuilder.Append(this.GetListLinesColumns(this.tableDescription, "I"));
            stringBuilder.AppendLine(" 0 as [sync_row_is_tombstone], 0 as [Level]");
            stringBuilder.AppendLine("FROM [INSERTED] as [I]");

            // Get inner joins from filter
            stringBuilder.Append(this.GetInnerJoins(this.tableDescription, "I"));

            stringBuilder.AppendLine("UNION");
            stringBuilder.Append("SELECT ");
            stringBuilder.Append(this.GetListLinesColumns(this.tableDescription, "D"));
            stringBuilder.AppendLine(" 1 as [sync_row_is_tombstone], 1 as [Level]");
            stringBuilder.AppendLine("FROM [DELETED] as [D]");
            stringBuilder.AppendLine($"LEFT JOIN [INSERTED] as [I] ON {SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[D]", "[I]")}");

            // Get inner joins from filter
            stringBuilder.Append(this.GetInnerJoins(this.tableDescription, "D"));

            stringBuilder.Append("WHERE ");
            string and = "";
            foreach (var pk in primaryKeys)
            {
                stringBuilder.Append($"{and}[I].{ParserName.Parse(pk).Quoted().ToString()} IS NULL");
                and = " AND ";
            }
            stringBuilder.AppendLine();

            stringBuilder.AppendLine("UNION");
            stringBuilder.Append("SELECT ");
            stringBuilder.Append(this.GetListLinesColumns(this.tableDescription, "I"));
            stringBuilder.AppendLine(" 0 as [sync_row_is_tombstone], 2 as [Level]");
            stringBuilder.AppendLine("FROM [INSERTED] as [I]");
            stringBuilder.AppendLine($"LEFT JOIN [DELETED] as [D] ON {SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[D]", "[I]")}");

            // Get inner joins from filter
            stringBuilder.Append(this.GetInnerJoins(this.tableDescription, "I"));
            stringBuilder.Append("WHERE ");
            and = "";
            foreach (var pk in primaryKeys)
            {
                stringBuilder.Append($"{and}[I].{ParserName.Parse(pk).Quoted().ToString()} IS NULL");
                and = " AND ";
            }
            stringBuilder.Append(";");
            stringBuilder.AppendLine();


            return stringBuilder.ToString();
        }


        private string CreateMergeStatement(SyncTable table, string level)
        {
            var stringBuilder = new StringBuilder();
            var (tableName, trackingName) = SqlTableBuilder.GetParsers(table);

            var lstColumns = this.GetPrimaryKeysForLinesTable(table);
            var baseListColumns = this.GetPrimaryKeysForLinesTable(this.tableDescription);

            stringBuilder.AppendLine($"WITH [side] AS (");
            stringBuilder.Append($"SELECT [LINES].* ");

            var primaryColumns = table.GetPrimaryKeysColumns();
            foreach (var pk in primaryColumns)
            {
                if (baseListColumns.Any(c => c == pk))
                    continue;

                stringBuilder.Append($", {tableName.Quoted()}.{ParserName.Parse(pk).Quoted().ToString()}");
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"FROM @LINES as [LINES] ");
            stringBuilder.Append($"{this.GetReverseInnerJoins(table, "LINES")}");
            stringBuilder.AppendLine($"WHERE [LINES].[Level] {level})");

            stringBuilder.AppendLine($"MERGE {trackingName} AS [base]");
            stringBuilder.AppendLine($"USING [side] on {SqlManagementUtils.JoinTwoTablesOnClause(lstColumns.Select(c => c.ColumnName), "[side]", "[base]")}");
            stringBuilder.AppendLine($"WHEN MATCHED THEN");
            stringBuilder.AppendLine($"  UPDATE SET [update_scope_id] = NULL ,[sync_row_is_tombstone] = [side].[sync_row_is_tombstone], [last_change_datetime] = GetUtcDate()");
            stringBuilder.AppendLine($"WHEN NOT MATCHED BY TARGET THEN");
            stringBuilder.Append($"  INSERT (");
            foreach (var c in lstColumns)
                stringBuilder.Append($"{ParserName.Parse(c).Quoted().ToString()}, ");
            stringBuilder.AppendLine($"[update_scope_id], [sync_row_is_tombstone], [last_change_datetime])");
            stringBuilder.Append($"  VALUES (");
            foreach (var c in lstColumns)
                stringBuilder.Append($"[side].{ParserName.Parse(c).Quoted().ToString()}, ");
            stringBuilder.AppendLine($"NULL, [side].[sync_row_is_tombstone], GetUtcDate());");

            return stringBuilder.ToString();
        }

        private string UpdateTriggerBodyText2()
        {
            var stringBuilder = new StringBuilder();

            stringBuilder.AppendLine(this.CreateLinesTableDeclaration());
            stringBuilder.AppendLine(this.CreateMergeStatement(this.tableDescription, "<= 1"));

            var schema = this.tableDescription.Schema;

            var tableDescriptionFilter = this.tableDescription.GetFilter();

            if (schema.Filters != null && schema.Filters.Count > 0)
            {
                foreach (var filter in schema.Filters)
                {
                    foreach (var p in filter.Parameters)
                    {
                        // Get where for this parameter
                        var where = filter.Wheres.FirstOrDefault(w => w.ParameterName == p.Name);

                        if (where == null)
                            continue;

                        var tableWhere = schema.Tables[where.TableName, where.SchemaName];

                        if (tableWhere == this.tableDescription)
                        {
                            var filterTable = schema.Tables[filter.TableName, filter.SchemaName];

                            if (filterTable != this.tableDescription)
                                stringBuilder.AppendLine(this.CreateMergeStatement(filterTable, ">= 1"));
                        }
                    }
                }
            }

            return stringBuilder.ToString();
        }


        private string UpdateTriggerBodyText()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("SET NOCOUNT ON;");
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
                        command.Transaction = this.transaction;

                    var updTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger).name;
                    var createTrigger = new StringBuilder($"CREATE TRIGGER {updTriggerName} ON {tableName.Schema().Quoted().ToString()} FOR UPDATE AS");
                    createTrigger.AppendLine();
                    createTrigger.AppendLine(this.UpdateTriggerBodyText());

                    var createTrigger2 = new StringBuilder($"CREATE TRIGGER {updTriggerName} ON {tableName.Schema().Quoted().ToString()} FOR UPDATE AS");
                    createTrigger2.AppendLine();
                    createTrigger2.AppendLine(this.UpdateTriggerBodyText2());
                    var strTrigger2 = createTrigger2.ToString();

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
