using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;
using Dotmim.Sync.SqlServer.Builders;
using Dotmim.Sync.SqlServer.Manager;
using Microsoft.Data.SqlClient;
using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.SqlServer.ChangeTracking.Builders
{

    /// <inheritdoc />
    public class SqlChangeTrackingBuilderProcedure : SqlBuilderProcedure
    {

        /// <inheritdoc />
        public SqlChangeTrackingBuilderProcedure(SyncTable tableDescription, SqlObjectNames sqlObjectNames, SqlDbMetadata sqlDbMetadata)
            : base(tableDescription, sqlObjectNames, sqlDbMetadata)
        {
        }

        /// <inheritdoc />
        protected override SqlCommand BuildBulkDeleteCommand()
        {
            var sqlCommand = new SqlCommand();

            var sqlParameter = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter);

            var sqlParameter1 = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter1);

            var sqlParameterForceWrite = new SqlParameter("@sync_force_write", SqlDbType.Int);
            sqlCommand.Parameters.Add(sqlParameterForceWrite);

            var bulkTypeName = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkTableType);
            var buljTypeParser = new ObjectParser(bulkTypeName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);

            var sqlParameter2 = new SqlParameter("@changeTable", SqlDbType.Structured)
            {
                TypeName = buljTypeParser.ObjectName,
            };
            sqlCommand.Parameters.Add(sqlParameter2);

            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[p]", "[CT]");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[changes]", "[base]");

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got deleted");
            stringBuilder.Append("declare @dms_changed TABLE (");
            foreach (var c in this.TableDescription.GetPrimaryKeysColumns())
            {
                // Get the good SqlDbType (even if we are not from Sql Server def)
                var columnType = this.SqlDbMetadata.GetCompatibleColumnTypeDeclarationString(c, this.TableDescription.OriginalProvider);
                var columnParser = new ObjectParser(c.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);

                stringBuilder.Append($"{columnParser.QuotedShortName} {columnType}, ");
            }

            stringBuilder.Append(" PRIMARY KEY (");
            var pkeyComma = " ";
            foreach (var primaryKeyColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(primaryKeyColumn.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
                stringBuilder.Append($"{pkeyComma}{columnParser.QuotedShortName}");
                pkeyComma = ", ";
            }

            stringBuilder.AppendLine("));");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"DECLARE @var_sync_scope_id varbinary(128) = cast(@sync_scope_id as varbinary(128));");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($";WITH ");
            stringBuilder.AppendLine($"  CHANGE_TRACKING_CONTEXT(@var_sync_scope_id),");
            stringBuilder.AppendLine($"  {this.SqlObjectNames.TrackingTableQuotedShortName} AS (");
            stringBuilder.Append($"\tSELECT ");
            foreach (var c in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(c.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
                stringBuilder.Append($"[p].{columnParser.QuotedShortName}, ");
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\tCAST([CT].[SYS_CHANGE_CONTEXT] as uniqueidentifier) AS [sync_update_scope_id], ");
            stringBuilder.AppendLine($"\t[CT].[SYS_CHANGE_VERSION] as [sync_timestamp],");
            stringBuilder.AppendLine($"\tCASE WHEN [CT].[SYS_CHANGE_OPERATION] = 'D' THEN 1 ELSE 0 END AS [sync_row_is_tombstone]");
            stringBuilder.AppendLine($"\tFROM @changeTable AS [p] ");
            stringBuilder.AppendLine($"\tLEFT JOIN CHANGETABLE(CHANGES {this.SqlObjectNames.TableQuotedFullName}, @sync_min_timestamp) AS [CT] ON {str4}");
            stringBuilder.AppendLine($"\t)");

            stringBuilder.AppendLine($"DELETE {this.SqlObjectNames.TableQuotedFullName}");
            stringBuilder.Append($"OUTPUT ");

            pkeyComma = " ";
            foreach (var primaryKeyColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(primaryKeyColumn.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
                stringBuilder.Append($"{pkeyComma}DELETED.{columnParser.QuotedShortName}");
                pkeyComma = ", ";
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"INTO @dms_changed ");
            stringBuilder.AppendLine($"FROM {this.SqlObjectNames.TableQuotedFullName} [base]");
            stringBuilder.AppendLine($"JOIN {this.SqlObjectNames.TrackingTableQuotedShortName} [changes] ON {str5}");
            stringBuilder.AppendLine("WHERE [changes].[sync_timestamp] <= @sync_min_timestamp OR [changes].[sync_timestamp] IS NULL OR [changes].[sync_update_scope_id] = @sync_scope_id OR @sync_force_write = 1;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine();
            stringBuilder.Append(this.BulkSelectUnsuccessfulRows());
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        /// <inheritdoc />
        protected override SqlCommand BuildBulkUpdateCommand(bool hasMutableColumns)
        {
            var sqlCommand = new SqlCommand();
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();
            var setupHasTableWithColumns = this.TableDescription.Columns.Count > 0;

            var sqlParameter = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter);

            var sqlParameter1 = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter1);

            var sqlParameterForceWrite = new SqlParameter("@sync_force_write", SqlDbType.Int);
            sqlCommand.Parameters.Add(sqlParameterForceWrite);

            var bulkTypeName = this.SqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkTableType);
            var buljTypeParser = new ObjectParser(bulkTypeName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);

            var sqlParameter2 = new SqlParameter("@changeTable", SqlDbType.Structured)
            {
                TypeName = buljTypeParser.ObjectName,
            };
            sqlCommand.Parameters.Add(sqlParameter2);

            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[CT]", "[p]");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[changes]", "[base]");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[t]", "[side]");
            string str7 = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[p]", "[side]");

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @dms_changed TABLE (");
            foreach (var c in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(c.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
                var columnType = this.SqlDbMetadata.GetCompatibleColumnTypeDeclarationString(c, this.TableDescription.OriginalProvider);

                stringBuilder.Append($"{columnParser.QuotedShortName} {columnType}, ");
            }

            stringBuilder.Append(" PRIMARY KEY (");
            var pkeyComma = " ";
            foreach (var primaryKeyColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(primaryKeyColumn.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
                stringBuilder.Append($"{pkeyComma}{columnParser.QuotedShortName}");
                pkeyComma = ", ";
            }

            stringBuilder.AppendLine("));");
            stringBuilder.AppendLine();

            // Check if we have auto inc column
            if (this.TableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {this.SqlObjectNames.TableQuotedFullName} ON;");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine("DECLARE @var_sync_scope_id varbinary(128) = cast(@sync_scope_id as varbinary(128));");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine(";WITH ");
            stringBuilder.AppendLine("  CHANGE_TRACKING_CONTEXT(@var_sync_scope_id),");
            stringBuilder.AppendLine($"  {this.SqlObjectNames.TrackingTableQuotedShortName} AS (");
            stringBuilder.Append("\tSELECT ");
            foreach (var c in this.TableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnParser = new ObjectParser(c.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
                stringBuilder.Append($"[p].{columnParser.QuotedShortName}, ");
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("\tCAST([CT].[SYS_CHANGE_CONTEXT] AS uniqueidentifier) AS [sync_update_scope_id],");
            stringBuilder.AppendLine("\t[CT].[SYS_CHANGE_VERSION] AS [sync_timestamp],");
            stringBuilder.Append("\tCASE WHEN [CT].[SYS_CHANGE_OPERATION] = 'D' THEN 1 ELSE 0 END AS [sync_row_is_tombstone]");
            if (setupHasTableWithColumns)
            {
                stringBuilder.Append(",\n\t[CT].[SYS_CHANGE_COLUMNS] AS [sync_change_columns]");
            }

            stringBuilder.AppendLine("\n\tFROM @changeTable AS [p]");
            stringBuilder.AppendLine($"\tLEFT JOIN CHANGETABLE(CHANGES {this.SqlObjectNames.TableQuotedFullName}, @sync_min_timestamp) AS [CT] ON {str4}");
            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine($"MERGE {this.SqlObjectNames.TableQuotedFullName} AS [base]");
            stringBuilder.AppendLine($"USING {this.SqlObjectNames.TrackingTableQuotedShortName} as [changes] ON {str5}");

            if (hasMutableColumns)
            {
                stringBuilder.AppendLine("WHEN MATCHED AND (");
                stringBuilder.AppendLine("\t[changes].[sync_timestamp] <= @sync_min_timestamp");
                stringBuilder.AppendLine("\tOR [changes].[sync_timestamp] IS NULL");
                stringBuilder.AppendLine("\tOR [changes].[sync_update_scope_id] = @sync_scope_id");
                stringBuilder.AppendLine("\tOR @sync_force_write = 1");

                stringBuilder.AppendLine(") THEN");
                stringBuilder.AppendLine("\tUPDATE SET");

                string strSeparator = string.Empty;
                foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false))
                {
                    var columnParser = new ObjectParser(mutableColumn.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
                    stringBuilder.AppendLine($"\t{strSeparator}{columnParser.QuotedShortName} = [changes].{columnParser.QuotedShortName}");
                    strSeparator = ", ";
                }
            }

            stringBuilder.AppendLine("WHEN NOT MATCHED BY TARGET AND ([changes].[sync_timestamp] <= @sync_min_timestamp OR [changes].[sync_timestamp] IS NULL OR @sync_force_write = 1) THEN");

            stringBuilderArguments = new StringBuilder();
            stringBuilderParameters = new StringBuilder();
            string empty = string.Empty;

            foreach (var mutableColumn in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);

                stringBuilderArguments.Append(string.Concat(empty, columnParser.QuotedShortName));
                stringBuilderParameters.Append(string.Concat(empty, $"[changes].{columnParser.QuotedShortName}"));
                empty = ", ";
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("\tINSERT");
            stringBuilder.AppendLine($"\t({stringBuilderArguments})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters})");
            stringBuilder.AppendLine();
            stringBuilder.Append("OUTPUT ");
            pkeyComma = " ";
            foreach (var primaryKeyColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(primaryKeyColumn.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
                stringBuilder.Append($"{pkeyComma}INSERTED.{columnParser.QuotedShortName}");
                pkeyComma = ", ";
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("\tINTO @dms_changed; -- populates the temp table with successful PKs");
            stringBuilder.AppendLine();

            // Check if we have auto inc column
            if (this.TableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {this.SqlObjectNames.TableQuotedFullName} OFF;");
                stringBuilder.AppendLine();
            }

            stringBuilder.Append(this.BulkSelectUnsuccessfulRows());

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        /// <inheritdoc/>
        protected override SqlCommand BuildDeleteCommand()
        {
            var sqlCommand = new SqlCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);
            var sqlParameter = new SqlParameter("@sync_force_write", SqlDbType.Int);
            sqlCommand.Parameters.Add(sqlParameter);
            var sqlParameter1 = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter1);

            var sqlParameter3 = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter3);

            var sqlParameter2 = new SqlParameter("@sync_row_count", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output,
            };
            sqlCommand.Parameters.Add(sqlParameter2);

            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[CT]", "[p]");

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"SET {sqlParameter2.ParameterName} = 0;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("DECLARE @var_sync_scope_id varbinary(128) = cast(@sync_scope_id as varbinary(128));");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine(";WITH ");
            stringBuilder.AppendLine("  CHANGE_TRACKING_CONTEXT(@var_sync_scope_id),");
            stringBuilder.AppendLine($"  {this.SqlObjectNames.TrackingTableQuotedShortName} AS (");
            stringBuilder.Append("\tSELECT ");
            foreach (var c in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(c.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
                stringBuilder.Append($"[p].{columnParser.QuotedShortName}, ");
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("\tCAST([CT].[SYS_CHANGE_CONTEXT] as uniqueidentifier) AS [sync_update_scope_id],");
            stringBuilder.AppendLine("\t[CT].[SYS_CHANGE_VERSION] as [sync_timestamp],");
            stringBuilder.AppendLine("\tCASE WHEN [CT].[SYS_CHANGE_OPERATION] = 'D' THEN 1 ELSE 0 END AS [sync_row_is_tombstone]");
            stringBuilder.Append("\tFROM (SELECT ");
            string comma = string.Empty;
            foreach (var c in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(c.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);

                stringBuilder.Append($"{comma}@{columnParser.NormalizedShortName} as {columnParser.QuotedShortName}");
                comma = ", ";
            }

            stringBuilder.AppendLine(") AS [p]");
            stringBuilder.Append($"\tLEFT JOIN CHANGETABLE(CHANGES {this.SqlObjectNames.TableQuotedFullName}, @sync_min_timestamp) AS [CT] ON {str4}");
            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine($"DELETE {this.SqlObjectNames.TableQuotedFullName}");
            stringBuilder.AppendLine($"FROM {this.SqlObjectNames.TableQuotedFullName} [base]");
            stringBuilder.Append($"JOIN {this.SqlObjectNames.TrackingTableQuotedShortName} [side] ON ");

            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[base]", "[side]"));

            stringBuilder.AppendLine("WHERE ([side].[sync_timestamp] <= @sync_min_timestamp OR [side].[sync_timestamp] IS NULL OR [side].[sync_update_scope_id] = @sync_scope_id OR @sync_force_write = 1)");
            stringBuilder.Append("AND ");
            stringBuilder.AppendLine(string.Concat("(", SqlManagementUtils.ColumnsAndParameters(this.TableDescription.PrimaryKeys, "[base]"), ");"));
            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("SET ", sqlParameter2.ParameterName, " = @@ROWCOUNT;"));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        /// <inheritdoc/>
        protected override SqlCommand BuildUpdateCommand(bool hasMutableColumns)
        {
            var sqlCommand = new SqlCommand();
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();
            var setupHasTableWithColumns = this.TableDescription.Columns.Count > 0;

            this.AddColumnParametersToCommand(sqlCommand);

            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[CT]", "[p]");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[changes]", "[base]");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[t]", "[side]");
            string str7 = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[p]", "[side]");

            var sqlParameter1 = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter1);

            var sqlParameter2 = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter2);

            var sqlParameter3 = new SqlParameter("@sync_force_write", SqlDbType.Int);
            sqlCommand.Parameters.Add(sqlParameter3);

            var sqlParameter4 = new SqlParameter("@sync_row_count", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output,
            };
            sqlCommand.Parameters.Add(sqlParameter4);

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();

            // Check if we have auto inc column
            if (this.TableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {this.SqlObjectNames.TableQuotedFullName} ON;");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine("DECLARE @var_sync_scope_id varbinary(128) = cast(@sync_scope_id as varbinary(128));");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"SET {sqlParameter4.ParameterName} = 0;");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine(";WITH ");
            stringBuilder.AppendLine("  CHANGE_TRACKING_CONTEXT(@var_sync_scope_id),");
            stringBuilder.AppendLine($"  {this.SqlObjectNames.TrackingTableQuotedShortName} AS (");
            stringBuilder.Append("\tSELECT ");
            foreach (var c in this.TableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnParser = new ObjectParser(c.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
                stringBuilder.Append($"[p].{columnParser.QuotedShortName}, ");
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("\tCAST([CT].[SYS_CHANGE_CONTEXT] as uniqueidentifier) AS [sync_update_scope_id],");
            stringBuilder.AppendLine("\t[CT].[SYS_CHANGE_VERSION] AS [sync_timestamp],");
            stringBuilder.Append("\tCASE WHEN [CT].[SYS_CHANGE_OPERATION] = 'D' THEN 1 ELSE 0 END AS [sync_row_is_tombstone]");
            if (setupHasTableWithColumns)
            {
                stringBuilder.Append(",\n\t[CT].[SYS_CHANGE_COLUMNS] AS [sync_change_columns]");
            }

            stringBuilder.AppendLine("\n\tFROM (SELECT ");
            stringBuilder.Append("\t\t ");
            string comma = string.Empty;
            foreach (var c in this.TableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnParser = new ObjectParser(c.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);

                stringBuilder.Append($"{comma}@{columnParser.NormalizedShortName}  as  {columnParser.QuotedShortName}");
                comma = ", ";
            }

            stringBuilder.AppendLine(") AS [p]");
            stringBuilder.Append($"\tLEFT JOIN CHANGETABLE(CHANGES {this.SqlObjectNames.TableQuotedFullName}, @sync_min_timestamp) AS [CT] ON {str4}");
            stringBuilder.AppendLine("\t)");

            stringBuilder.AppendLine($"MERGE {this.SqlObjectNames.TableQuotedFullName} AS [base]");
            stringBuilder.AppendLine($"USING {this.SqlObjectNames.TrackingTableQuotedShortName} as [changes] ON {str5}");

            if (hasMutableColumns)
            {
                stringBuilder.AppendLine("WHEN MATCHED AND (");
                stringBuilder.AppendLine("\t[changes].[sync_timestamp] <= @sync_min_timestamp");
                stringBuilder.AppendLine("\tOR [changes].[sync_timestamp] IS NULL");
                stringBuilder.AppendLine("\tOR [changes].[sync_update_scope_id] = @sync_scope_id");
                stringBuilder.AppendLine("\tOR @sync_force_write = 1");

                stringBuilder.AppendLine(") THEN");
                stringBuilder.AppendLine("\tUPDATE SET");

                string strSeparator = string.Empty;
                foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false))
                {
                    var columnParser = new ObjectParser(mutableColumn.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
                    stringBuilder.AppendLine($"\t{strSeparator}{columnParser.QuotedShortName} = [changes].{columnParser.QuotedShortName}");
                    strSeparator = ", ";
                }
            }

            stringBuilder.AppendLine("WHEN NOT MATCHED BY TARGET AND ([changes].[sync_timestamp] <= @sync_min_timestamp OR [changes].[sync_timestamp] IS NULL OR @sync_force_write = 1) THEN");

            stringBuilderArguments = new StringBuilder();
            stringBuilderParameters = new StringBuilder();
            var empty = string.Empty;

            foreach (var mutableColumn in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);

                stringBuilderArguments.Append(string.Concat(empty, columnParser.QuotedShortName));
                stringBuilderParameters.Append(string.Concat(empty, $"[changes].{columnParser.QuotedShortName}"));
                empty = ", ";
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("\tINSERT");
            stringBuilder.AppendLine($"\t({stringBuilderArguments})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters});");
            stringBuilder.AppendLine();

            // GET row count BEFORE make identity insert off again
            stringBuilder.AppendLine($"SET {sqlParameter4.ParameterName} = @@ROWCOUNT;");

            // Check if we have auto inc column
            if (this.TableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {this.SqlObjectNames.TableQuotedFullName} OFF;");
                stringBuilder.AppendLine();
            }

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        /// <inheritdoc/>
        protected override SqlCommand BuildSelectInitializedChangesCommand(DbConnection connection, DbTransaction transaction, SyncFilter filter = null)
        {
            // Initialize SQL command with timestamp parameter
            var sqlCommand = new SqlCommand();
            sqlCommand.Parameters.Add(new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt) { Value = "NULL", IsNullable = true });

            // Add any filter parameters (e.g., TenantId for multi-tenant scenarios)
            if (filter != null)
                this.CreateFilterParameters(sqlCommand, filter);

            var sb = new StringBuilder();
            sb.AppendLine("SET NOCOUNT ON;"); // Reduce network traffic by suppressing row count messages
            sb.AppendLine();

            // ====================================================================
            // Helper method: Builds the SELECT column list for a query
            // Parameters:
            //   - tableAlias: The alias to use for table references (e.g., "base", "side")
            //   - includeCoalesce: If true, wraps primary keys in COALESCE for tombstone handling
            // ====================================================================
            string BuildSelectColumns(string tableAlias, bool includeCoalesce = false)
            {
                var columns = new StringBuilder();
                var comma = "  ";

                // Iterate through all mutable columns (excludes computed/identity columns)
                foreach (var col in this.TableDescription.GetMutableColumns(false, true))
                {
                    var colName = new ObjectParser(col.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote).QuotedShortName;
                    var isPrimaryKey = this.TableDescription.PrimaryKeys.Any(pk => col.ColumnName.Equals(pk, SyncGlobalization.DataSourceStringComparison));

                    // For incremental sync, primary keys need COALESCE to handle deleted rows (tombstones)
                    // Non-PK columns come from [base] table only (NULL for tombstones)
                    if (includeCoalesce && isPrimaryKey)
                        columns.AppendLine($"\t{comma}COALESCE([base].{colName}, [side].{colName}) AS {colName}");
                    else
                        columns.AppendLine($"\t{comma}[{tableAlias}].{colName}");

                    comma = ", ";
                }

                return columns.ToString();
            }

            // ====================================================================
            // FULL SYNC PATH - Used when @sync_min_timestamp IS NULL
            // ====================================================================
            // This path is optimized for initial synchronization where all data
            // needs to be transferred. It bypasses change tracking entirely for
            // maximum performance, doing a simple SELECT from the base table.
            // ====================================================================

            sb.AppendLine("IF @sync_min_timestamp IS NULL");
            sb.AppendLine("BEGIN");

            // Use DISTINCT only if filters might create duplicate rows via joins
            sb.AppendLine(filter != null ? "\tSELECT DISTINCT" : "\tSELECT");

            // Select all columns from the base table
            sb.Append(BuildSelectColumns("base"));

            // No tombstones in full sync - all rows are live data
            sb.AppendLine("\t\t, 0 AS [sync_row_is_tombstone]");
            sb.AppendLine($"\tFROM {this.SqlObjectNames.TableQuotedFullName} [base]");

            // Apply filters (e.g., WHERE TenantId = @TenantId)
            if (filter != null)
            {
                sb.AppendLine("\tWHERE (");

                // CreateFilterWhereSide generates conditions like "[base].[TenantId] = @TenantId"
                sb.Append($"\t\t{this.CreateFilterWhereSide(filter, false)}");

                // Add any custom WHERE conditions defined in the filter
                var customWheres = this.CreateFilterCustomWheres(filter);
                if (!string.IsNullOrEmpty(customWheres))
                {
                    sb.AppendLine();
                    sb.AppendLine("\t\tAND");
                    sb.Append($"\t\t{customWheres}");
                }

                sb.AppendLine();
                sb.AppendLine("\t)");
            }

            sb.AppendLine("\t;");
            sb.AppendLine("\tRETURN;"); // Exit early - don't execute incremental sync path
            sb.AppendLine("END");
            sb.AppendLine();

            // ====================================================================
            // INCREMENTAL SYNC PATH - Used when @sync_min_timestamp has a value
            // ====================================================================
            // This path uses SQL Server Change Tracking to identify only the rows
            // that have changed since the last sync. This is much more efficient
            // for ongoing synchronization after the initial load.
            // ====================================================================

            // Build CTE that queries change tracking for modified rows
            sb.AppendLine(";WITH");
            sb.AppendLine($"  {this.SqlObjectNames.TrackingTableQuotedShortName} AS (");
            sb.Append("\tSELECT ");

            // Select primary key columns from change tracking
            // We need PKs to join back to the base table
            var pkColumns = this.TableDescription.GetPrimaryKeysColumns();
            foreach (var pkCol in pkColumns)
            {
                var colName = new ObjectParser(pkCol.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote).QuotedShortName;
                sb.Append($"[CT].{colName}, ");
            }

            sb.AppendLine();

            // SYS_CHANGE_VERSION is the timestamp for this change
            sb.AppendLine("\t[CT].[SYS_CHANGE_VERSION] AS [sync_timestamp],");

            // SYS_CHANGE_OPERATION: 'I'=Insert, 'U'=Update, 'D'=Delete
            // Mark deleted rows as tombstones (sync_row_is_tombstone = 1)
            sb.AppendLine("\tCASE WHEN [CT].[SYS_CHANGE_OPERATION] = 'D' THEN 1 ELSE 0 END AS [sync_row_is_tombstone]");

            // CHANGETABLE function returns all changes after the specified timestamp
            sb.AppendLine($"\tFROM CHANGETABLE(CHANGES {this.SqlObjectNames.TableQuotedFullName}, @sync_min_timestamp) AS [CT]");

            // Filter change tracking results
            var ctWhere = new StringBuilder("\tWHERE ");
            var hasFilter = false;

            // Apply filters to change tracking if all filter parameters are primary keys
            // This optimization pushes filtering into the change tracking query
            if (filter != null && filter.Parameters.All(x => pkColumns.Any(y => y.ColumnName == x.Name)))
            {
                var filterWhere = this.CreateFilterWhereSide(filter, false)
                    .Replace("[base]", "[CT]", StringComparison.CurrentCultureIgnoreCase); // Replace table alias
                ctWhere.Append(filterWhere);
                ctWhere.AppendLine();
                ctWhere.Append("\t\tAND ");
                hasFilter = true;
            }

            // Only get changes after the last sync timestamp
            // This is the core of incremental sync efficiency
            ctWhere.AppendLine("[CT].[SYS_CHANGE_VERSION] > @sync_min_timestamp");
            sb.Append(ctWhere);
            sb.AppendLine("\t)");

            // ====================================================================
            // Main query: Join change tracking results with base table
            // ====================================================================
            // Strategy: Start from the tracking CTE (small dataset) and LEFT JOIN
            // to the base table. This handles both live rows and tombstones:
            // - Live rows: JOIN succeeds, get full data from [base]
            // - Tombstones: JOIN fails (row deleted), but we still have PKs from [side]
            // ====================================================================

            sb.AppendLine(filter != null ? "SELECT DISTINCT" : "SELECT");

            // Build column list with COALESCE on primary keys to handle tombstones
            sb.Append(BuildSelectColumns("base", includeCoalesce: true));

            // Return the tombstone flag from change tracking, default to 0 if NULL
            sb.AppendLine("\t, COALESCE([side].[sync_row_is_tombstone], 0) AS [sync_row_is_tombstone]");

            // Start from tracking table (smaller dataset) - performance optimization
            sb.AppendLine($"FROM {this.SqlObjectNames.TrackingTableQuotedShortName} [side]");

            // LEFT JOIN to base table - will be NULL for deleted rows (tombstones)
            // NOLOCK hint reduces blocking (acceptable for sync scenarios with eventual consistency)
            sb.Append($"LEFT JOIN {this.SqlObjectNames.TableQuotedFullName} [base] WITH (NOLOCK) ON ");

            // Build join condition on all primary key columns
            var joinConditions = pkColumns.Select(pk =>
            {
                var colName = new ObjectParser(pk.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote).QuotedShortName;
                return $"[base].{colName} = [side].{colName}";
            });
            sb.Append(string.Join(" AND ", joinConditions));

            // Apply any custom joins and WHERE clauses from the filter
            if (filter != null)
            {
                // Custom joins might be needed for complex filters
                sb.Append(this.CreateFilterCustomJoins(filter));

                // Additional WHERE conditions beyond the primary key filter
                var customWheres = this.CreateFilterCustomWheres(filter);
                if (!string.IsNullOrEmpty(customWheres))
                {
                    sb.AppendLine();
                    sb.AppendLine($"WHERE ({customWheres})");
                }
            }

            sb.AppendLine(";");
            
            sqlCommand.CommandText = sb.ToString();
            
            return sqlCommand;
        }

       
       
        /// <inheritdoc/>
        protected override SqlCommand BuildSelectIncrementalChangesCommand(SyncFilter filter)
        {
            var sqlCommand = new SqlCommand();
            var pTimestamp = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            var pScopeId = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            var setupHasTableWithColumns = this.TableDescription.Columns.Count > 0;

            sqlCommand.Parameters.Add(pTimestamp);
            sqlCommand.Parameters.Add(pScopeId);

            // Add filter parameters
            if (filter != null)
                this.CreateFilterParameters(sqlCommand, filter);

            var stringBuilder = new StringBuilder(string.Empty);
            stringBuilder.AppendLine(";WITH ");
            stringBuilder.AppendLine($"  {this.SqlObjectNames.TrackingTableQuotedShortName} AS (");
            stringBuilder.Append("\tSELECT ");
            var pkColumns = this.TableDescription.GetPrimaryKeysColumns();
            foreach (var pkColumn in pkColumns)
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
                stringBuilder.Append($"[CT].{columnParser.QuotedShortName}, ");
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("\tCAST([CT].[SYS_CHANGE_CONTEXT] AS uniqueidentifier) AS [sync_update_scope_id],");
            stringBuilder.AppendLine("\t[CT].[SYS_CHANGE_VERSION] AS [sync_timestamp],");
            stringBuilder.Append("\tCASE WHEN [CT].[SYS_CHANGE_OPERATION] = 'D' THEN 1 ELSE 0 END AS [sync_row_is_tombstone]");
            if (setupHasTableWithColumns)
            {
                stringBuilder.Append(",\n\t[CT].[SYS_CHANGE_COLUMNS] AS [sync_change_columns]");
            }

            stringBuilder.AppendLine($"\n\tFROM CHANGETABLE(CHANGES {this.SqlObjectNames.TableQuotedFullName}, @sync_min_timestamp) AS [CT]");

            if ((filter != null) && (filter.Parameters.All(x => pkColumns.Any(y => y.ColumnName == x.Name))))
            {
                var createFilterWhereSide = this.CreateFilterWhereSide(filter, false);
                if (createFilterWhereSide.Contains("[base]", StringComparison.CurrentCultureIgnoreCase))
                {
                    createFilterWhereSide = createFilterWhereSide.Replace("[base]", "[CT]", StringComparison.CurrentCultureIgnoreCase);
                }
                stringBuilder.Append($" WHERE {createFilterWhereSide}");
            }

            stringBuilder.AppendLine("\t)");

            stringBuilder.AppendLine("SELECT DISTINCT");

            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);

                var isPrimaryKey = this.TableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.AppendLine($"\t[side].{columnParser.QuotedShortName}, ");
                else
                    stringBuilder.AppendLine($"\t[base].{columnParser.QuotedShortName}, ");
            }

            stringBuilder.AppendLine("\t[side].[sync_row_is_tombstone],");
            stringBuilder.AppendLine("\t[side].[sync_update_scope_id]");
            stringBuilder.AppendLine($"FROM {this.SqlObjectNames.TableQuotedFullName} [base]");
            stringBuilder.Append($"RIGHT JOIN {this.SqlObjectNames.TrackingTableQuotedShortName} [side]");
            stringBuilder.Append("ON ");

            string empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
                stringBuilder.Append($"{empty}[base].{columnParser.QuotedShortName} = [side].{columnParser.QuotedShortName}");
                empty = " AND ";
            }

            // ----------------------------------
            // Custom Joins
            // ----------------------------------
            if (filter != null)
                stringBuilder.Append(this.CreateFilterCustomJoins(filter));

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE (");

            // ----------------------------------
            // Where filters on [side]
            // ----------------------------------
            if (filter != null)
            {
                var createFilterWhereSide = this.CreateFilterWhereSide(filter, true);
                stringBuilder.Append(createFilterWhereSide);

                if (!string.IsNullOrEmpty(createFilterWhereSide))
                    stringBuilder.AppendLine("AND ");
            }

            // ----------------------------------

            // ----------------------------------
            // Custom Where
            // ----------------------------------
            if (filter != null)
            {
                var createFilterCustomWheres = this.CreateFilterCustomWheres(filter);
                stringBuilder.Append(createFilterCustomWheres);

                if (!string.IsNullOrEmpty(createFilterCustomWheres))
                    stringBuilder.AppendLine("AND ");
            }

            // ----------------------------------
            stringBuilder.AppendLine("\t[side].[sync_timestamp] > @sync_min_timestamp");
            stringBuilder.AppendLine("\tAND ([side].[sync_update_scope_id] <> @sync_scope_id OR [side].[sync_update_scope_id] IS NULL)");

            stringBuilder.AppendLine(")");
                       
            sqlCommand.CommandText = stringBuilder.ToString();

            return sqlCommand;
        }
    }
}