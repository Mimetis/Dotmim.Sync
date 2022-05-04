using Dotmim.Sync.Builders;
using Dotmim.Sync.SqlServer.Builders;
using Dotmim.Sync.SqlServer.Manager;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.SqlServer.ChangeTracking.Builders
{
    public class SqlChangeTrackingBuilderProcedure : SqlBuilderProcedure
    {
        private ParserName tableName;
        private ParserName trackingName;

        private readonly SyncTable tableDescription;
        private readonly SyncSetup setup;
        private readonly SqlObjectNames sqlObjectNames;
        private readonly SqlDbMetadata sqlDbMetadata;

        public SqlChangeTrackingBuilderProcedure(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup, string scopeName)
            : base(tableDescription, tableName, trackingName, setup, scopeName)
        {
            this.tableDescription = tableDescription;
            this.setup = setup;
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.sqlObjectNames = new SqlObjectNames(this.tableDescription, tableName, trackingName, this.setup, scopeName);
            this.sqlDbMetadata = new SqlDbMetadata();
        }

        //------------------------------------------------------------------
        // Bulk Delete command
        //------------------------------------------------------------------
        protected override SqlCommand BuildBulkDeleteCommand()
        {
            var sqlCommand = new SqlCommand();

            var sqlParameter = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter);

            var sqlParameter1 = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter1);

            var sqlParameter2 = new SqlParameter("@changeTable", SqlDbType.Structured)
            {
                TypeName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkTableType)
            };
            sqlCommand.Parameters.Add(sqlParameter2);

            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[p]", "[CT]");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[changes]", "[base]");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[t]", "[side]");
            string str7 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[p]", "[side]");

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got deleted");
            stringBuilder.Append("declare @dms_changed TABLE (");
            foreach (var c in this.tableDescription.GetPrimaryKeysColumns())
            {
                // Get the good SqlDbType (even if we are not from Sql Server def)
                var columnType = this.sqlDbMetadata.GetCompatibleColumnTypeDeclarationString(c, this.tableDescription.OriginalProvider);

                stringBuilder.Append($"{ParserName.Parse(c).Quoted().ToString()} {columnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");
            var pkeyComma = " ";
            foreach (var primaryKeyColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(primaryKeyColumn).Quoted().ToString();
                stringBuilder.Append($"{pkeyComma}{columnName}");
                pkeyComma = ", ";
            }

            stringBuilder.AppendLine("));");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"DECLARE @var_sync_scope_id varbinary(128) = cast(@sync_scope_id as varbinary(128));");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($";WITH ");
            stringBuilder.AppendLine($"  CHANGE_TRACKING_CONTEXT(@var_sync_scope_id),");
            stringBuilder.AppendLine($"  {trackingName.Quoted().ToString()} AS (");
            stringBuilder.Append($"\tSELECT ");
            foreach (var c in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"[p].{columnName}, ");
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\tCAST([CT].[SYS_CHANGE_CONTEXT] as uniqueidentifier) AS [sync_update_scope_id], ");
            stringBuilder.AppendLine($"\t[CT].[SYS_CHANGE_VERSION] as [sync_timestamp],");
            stringBuilder.AppendLine($"\tCASE WHEN [CT].[SYS_CHANGE_OPERATION] = 'D' THEN 1 ELSE 0 END AS [sync_row_is_tombstone]");
            stringBuilder.AppendLine($"\tFROM @changeTable AS [p] ");
            stringBuilder.AppendLine($"\tLEFT JOIN CHANGETABLE(CHANGES {tableName.Schema().Quoted().ToString()}, @sync_min_timestamp) AS [CT] ON {str4}");
            stringBuilder.AppendLine($"\t)");


            stringBuilder.AppendLine($"DELETE {tableName.Schema().Quoted().ToString()}");
            stringBuilder.Append($"OUTPUT ");

            pkeyComma = " ";
            foreach (var primaryKeyColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(primaryKeyColumn).Quoted().ToString();
                stringBuilder.Append($"{pkeyComma}DELETED.{columnName}");
                pkeyComma = ", ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"INTO @dms_changed ");
            stringBuilder.AppendLine($"FROM {tableName.Quoted().ToString()} [base]");
            stringBuilder.AppendLine($"JOIN {trackingName.Quoted().ToString()} [changes] ON {str5}");
            stringBuilder.AppendLine("WHERE [changes].[sync_timestamp] <= @sync_min_timestamp OR [changes].[sync_timestamp] IS NULL OR [changes].[sync_update_scope_id] = @sync_scope_id;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine();
            stringBuilder.Append(BulkSelectUnsuccessfulRows());
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        //------------------------------------------------------------------
        // Bulk Update command
        //------------------------------------------------------------------
        protected override SqlCommand BuildBulkUpdateCommand(bool hasMutableColumns)
        {
            var sqlCommand = new SqlCommand();
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();
            var setupHasTableWithColumns = setup.HasTableWithColumns(tableDescription.TableName);

            var sqlParameter = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter);

            var sqlParameter1 = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter1);

            var sqlParameter2 = new SqlParameter("@changeTable", SqlDbType.Structured)
            {
                TypeName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkTableType)
            };
            sqlCommand.Parameters.Add(sqlParameter2);

            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[CT]", "[p]");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[changes]", "[base]");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[t]", "[side]");
            string str7 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[p]", "[side]");

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @dms_changed TABLE (");
            foreach (var c in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                var columnType = this.sqlDbMetadata.GetCompatibleColumnTypeDeclarationString(c, this.tableDescription.OriginalProvider);

                stringBuilder.Append($"{columnName} {columnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");
            var pkeyComma = " ";
            foreach (var primaryKeyColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(primaryKeyColumn).Quoted().ToString();
                stringBuilder.Append($"{pkeyComma}{columnName}");
                pkeyComma = ", ";
            }

            stringBuilder.AppendLine("));");
            stringBuilder.AppendLine();

            // Check if we have auto inc column
            if (this.tableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {tableName.Schema().Quoted().ToString()} ON;");
                stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine("DECLARE @var_sync_scope_id varbinary(128) = cast(@sync_scope_id as varbinary(128));");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine(";WITH ");
            stringBuilder.AppendLine("  CHANGE_TRACKING_CONTEXT(@var_sync_scope_id),");
            stringBuilder.AppendLine($"  {trackingName.Quoted().ToString()} AS (");
            stringBuilder.Append("\tSELECT ");
            foreach (var c in this.tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"[p].{columnName}, ");
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
            stringBuilder.AppendLine($"\tLEFT JOIN CHANGETABLE(CHANGES {tableName.Schema().Quoted().ToString()}, @sync_min_timestamp) AS [CT] ON {str4}");
            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine($"MERGE {tableName.Schema().Quoted().ToString()} AS [base]");
            stringBuilder.AppendLine($"USING {trackingName.Quoted().ToString()} as [changes] ON {str5}");

            if (hasMutableColumns)
            {
                stringBuilder.AppendLine("WHEN MATCHED AND (");
                stringBuilder.AppendLine("\t[changes].[sync_timestamp] <= @sync_min_timestamp");
                stringBuilder.AppendLine("\tOR [changes].[sync_timestamp] IS NULL");
                stringBuilder.AppendLine("\tOR [changes].[sync_update_scope_id] = @sync_scope_id");
                //if (setupHasTableWithColumns)
                //{
                //    stringBuilder.AppendLine("\tOR (");
                //    string and = string.Empty;
                //    foreach (var column in this.tableDescription.GetMutableColumns())
                //    {
                //        var unquotedColumnName = ParserName.Parse(column).Unquoted().ToString();
                //        stringBuilder.Append("\t\t");
                //        stringBuilder.Append(and);
                //        stringBuilder.Append("CHANGE_TRACKING_IS_COLUMN_IN_MASK(");
                //        stringBuilder.Append($"COLUMNPROPERTY(OBJECT_ID('{tableName.Schema().Quoted().ToString()}'), '{unquotedColumnName}', 'ColumnId')");
                //        stringBuilder.AppendLine(", [changes].[sync_change_columns]) = 0");
                //        and = " AND ";
                //    }
                //    stringBuilder.AppendLine("\t)");
                //}

                stringBuilder.AppendLine(") THEN");
                stringBuilder.AppendLine("\tUPDATE SET");

                string strSeparator = string.Empty;
                foreach (var mutableColumn in this.tableDescription.GetMutableColumns(false))
                {
                    var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                    stringBuilder.AppendLine($"\t{strSeparator}{columnName} = [changes].{columnName}");
                    strSeparator = ", ";
                }
            }

            stringBuilder.AppendLine("WHEN NOT MATCHED BY TARGET AND ([changes].[sync_timestamp] <= @sync_min_timestamp OR [changes].[sync_timestamp] IS NULL) THEN");


            stringBuilderArguments = new StringBuilder();
            stringBuilderParameters = new StringBuilder();
            string empty = string.Empty;

            foreach (var mutableColumn in this.tableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();

                stringBuilderArguments.Append(string.Concat(empty, columnName));
                stringBuilderParameters.Append(string.Concat(empty, $"[changes].{columnName}"));
                empty = ", ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("\tINSERT");
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()})");
            stringBuilder.AppendLine();
            stringBuilder.Append("OUTPUT ");
            pkeyComma = " ";
            foreach (var primaryKeyColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(primaryKeyColumn).Quoted().ToString();
                stringBuilder.Append($"{pkeyComma}INSERTED.{columnName}");
                pkeyComma = ", ";
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("\tINTO @dms_changed; -- populates the temp table with successful PKs");
            stringBuilder.AppendLine();

            // Check if we have auto inc column
            if (this.tableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {tableName.Schema().Quoted().ToString()} OFF;");
                stringBuilder.AppendLine();
            }

            stringBuilder.Append(BulkSelectUnsuccessfulRows());

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        //------------------------------------------------------------------
        // Delete command
        //------------------------------------------------------------------
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
                Direction = ParameterDirection.Output
            };
            sqlCommand.Parameters.Add(sqlParameter2);

            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[CT]", "[p]");

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"SET {sqlParameter2.ParameterName} = 0;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("DECLARE @var_sync_scope_id varbinary(128) = cast(@sync_scope_id as varbinary(128));");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine(";WITH ");
            stringBuilder.AppendLine("  CHANGE_TRACKING_CONTEXT(@var_sync_scope_id),");
            stringBuilder.AppendLine($"  {trackingName.Quoted().ToString()} AS (");
            stringBuilder.Append("\tSELECT ");
            foreach (var c in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"[p].{columnName}, ");
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("\tCAST([CT].[SYS_CHANGE_CONTEXT] as uniqueidentifier) AS [sync_update_scope_id],");
            stringBuilder.AppendLine("\t[CT].[SYS_CHANGE_VERSION] as [sync_timestamp],");
            stringBuilder.AppendLine("\tCASE WHEN [CT].[SYS_CHANGE_OPERATION] = 'D' THEN 1 ELSE 0 END AS [sync_row_is_tombstone]");
            stringBuilder.Append("\tFROM (SELECT ");
            string comma = string.Empty;
            foreach (var c in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                var columnParameterName = ParserName.Parse(c).Unquoted().Normalized().ToString();

                stringBuilder.Append($"{comma}@{columnParameterName} as {columnName}");
                comma = ", ";
            }
            stringBuilder.AppendLine(") AS [p]");
            stringBuilder.Append($"\tLEFT JOIN CHANGETABLE(CHANGES {tableName.Schema().Quoted().ToString()}, @sync_min_timestamp) AS [CT] ON {str4}");
            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine($"DELETE {tableName.Schema().Quoted().ToString()}");
            stringBuilder.AppendLine($"FROM {tableName.Schema().Quoted().ToString()} [base]");
            stringBuilder.Append($"JOIN {trackingName.Quoted().ToString()} [side] ON ");

            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[base]", "[side]"));

            stringBuilder.AppendLine("WHERE ([side].[sync_timestamp] <= @sync_min_timestamp OR [side].[sync_timestamp] IS NULL OR [side].[sync_update_scope_id] = @sync_scope_id OR @sync_force_write = 1)");
            stringBuilder.Append("AND ");
            stringBuilder.AppendLine(string.Concat("(", SqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKeys, "[base]"), ");"));
            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("SET ", sqlParameter2.ParameterName, " = @@ROWCOUNT;"));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        //------------------------------------------------------------------
        // Select Row command
        //------------------------------------------------------------------
        protected override SqlCommand BuildSelectRowCommand()
        {
            var sqlCommand = new SqlCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);
            var sqlParameter = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter);

            var stringBuilder1 = new StringBuilder();
            var stringBuilder11 = new StringBuilder();
            var stringBuilder3 = new StringBuilder();
            var stringBuilder4 = new StringBuilder();

            string empty = string.Empty;
            string comma = string.Empty;
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();

                stringBuilder1.Append($"{empty}[side].{columnName} = @{parameterName}");
                stringBuilder11.Append($"{empty}[base].{columnName} = @{parameterName}");
                stringBuilder3.Append($"{comma}{columnName}");
                stringBuilder4.Append($"{empty}[base].{columnName} = [side].{columnName}");

                empty = " AND ";
                comma = ", ";
            }

            var stringBuilderColumnsWithSide = new StringBuilder();
            var stringBuilderColumnsBase = new StringBuilder();
            foreach (var mutableColumn in this.tableDescription.GetMutableColumns(false, true))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();

                var isPrimaryKey = this.tableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilderColumnsWithSide.AppendLine($"\t[side].{columnName}, ");
                else
                    stringBuilderColumnsWithSide.AppendLine($"\t[base].{columnName}, ");

                stringBuilderColumnsBase.AppendLine($"\t[base].{ columnName}, ");
            }

            var stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"IF (SELECT TOP 1 1 FROM CHANGETABLE(CHANGES {tableName.Schema().Quoted().ToString()}, 0) AS [side] WHERE ({stringBuilder1.ToString()})) > 0");
            stringBuilder.AppendLine("BEGIN");
            stringBuilder.AppendLine("\tSELECT");
            // add columns
            stringBuilder.Append(stringBuilderColumnsWithSide.ToString());
            stringBuilder.AppendLine("\tCAST([side].SYS_CHANGE_CONTEXT as uniqueidentifier) AS [sync_update_scope_id],");
            stringBuilder.AppendLine("\tCASE [side].SYS_CHANGE_OPERATION WHEN 'D' THEN 1 ELSE 0 END AS [sync_row_is_tombstone]");
            stringBuilder.AppendLine($"\tFROM CHANGETABLE(CHANGES {tableName.Schema().Quoted().ToString()}, 0) AS [side]");
            stringBuilder.AppendLine($"\tLEFT JOIN {tableName.Schema().Quoted().ToString()} [base] ON");
            stringBuilder.AppendLine($"\t\t{stringBuilder4.ToString()}");
            stringBuilder.AppendLine($"\tWHERE {stringBuilder1.ToString()}");
            stringBuilder.AppendLine("END");
            stringBuilder.AppendLine("ELSE");
            stringBuilder.AppendLine("BEGIN");
            stringBuilder.AppendLine("\tSELECT");
            stringBuilder.Append(stringBuilderColumnsBase.ToString());
            stringBuilder.AppendLine("\tnull as sync_update_scope_id, ");
            stringBuilder.AppendLine("\t0 as sync_row_is_tombstone ");
            stringBuilder.AppendLine($"\tFROM {tableName.Schema().Quoted().ToString()} as [base] ");
            stringBuilder.Append(string.Concat("\tWHERE ", stringBuilder11.ToString()));
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("END");

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        //------------------------------------------------------------------
        // Update command
        //------------------------------------------------------------------
        protected override SqlCommand BuildUpdateCommand(bool hasMutableColumns)
        {
            var sqlCommand = new SqlCommand();
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();
            var setupHasTableWithColumns = setup.HasTableWithColumns(tableDescription.TableName);

            this.AddColumnParametersToCommand(sqlCommand);

            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[CT]", "[p]");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[changes]", "[base]");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[t]", "[side]");
            string str7 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[p]", "[side]");

            var sqlParameter1 = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter1);

            var sqlParameter2 = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter2);

            var sqlParameter3 = new SqlParameter("@sync_force_write", SqlDbType.Int);
            sqlCommand.Parameters.Add(sqlParameter3);

            var sqlParameter4 = new SqlParameter("@sync_row_count", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output
            };
            sqlCommand.Parameters.Add(sqlParameter4);

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();

            // Check if we have auto inc column
            if (this.tableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {tableName.Schema().Quoted().ToString()} ON;");
                stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine("DECLARE @var_sync_scope_id varbinary(128) = cast(@sync_scope_id as varbinary(128));");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"SET {sqlParameter4.ParameterName} = 0;");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine(";WITH ");
            stringBuilder.AppendLine("  CHANGE_TRACKING_CONTEXT(@var_sync_scope_id),");
            stringBuilder.AppendLine($"  {trackingName.Quoted().ToString()} AS (");
            stringBuilder.Append("\tSELECT ");
            foreach (var c in this.tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"[p].{columnName}, ");
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
            foreach (var c in this.tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                var columnParameterName = ParserName.Parse(c).Unquoted().Normalized().ToString();

                stringBuilder.Append($"{comma}@{columnParameterName} as {columnName}");
                comma = ", ";
            }
            stringBuilder.AppendLine(") AS [p]");
            stringBuilder.Append($"\tLEFT JOIN CHANGETABLE(CHANGES {tableName.Schema().Quoted().ToString()}, @sync_min_timestamp) AS [CT] ON {str4}");
            stringBuilder.AppendLine("\t)");

            stringBuilder.AppendLine($"MERGE {tableName.Schema().Quoted().ToString()} AS [base]");
            stringBuilder.AppendLine($"USING {trackingName.Quoted().ToString()} as [changes] ON {str5}");

            if (hasMutableColumns)
            {
                stringBuilder.AppendLine("WHEN MATCHED AND (");
                stringBuilder.AppendLine("\t[changes].[sync_timestamp] <= @sync_min_timestamp");
                stringBuilder.AppendLine("\tOR [changes].[sync_timestamp] IS NULL");
                stringBuilder.AppendLine("\tOR [changes].[sync_update_scope_id] = @sync_scope_id");
                stringBuilder.AppendLine("\tOR @sync_force_write = 1");
                //if (setupHasTableWithColumns)
                //{
                //    stringBuilder.AppendLine("\tOR (");
                //    string and = string.Empty;
                //    foreach (var column in this.tableDescription.GetMutableColumns())
                //    {
                //        var unquotedColumnName = ParserName.Parse(column).Unquoted().ToString();
                //        stringBuilder.Append("\t\t");
                //        stringBuilder.Append(and);
                //        stringBuilder.Append("CHANGE_TRACKING_IS_COLUMN_IN_MASK(");
                //        stringBuilder.Append($"COLUMNPROPERTY(OBJECT_ID('{tableName.Schema().Quoted().ToString()}'), '{unquotedColumnName}', 'ColumnId')");
                //        stringBuilder.AppendLine(", [changes].[sync_change_columns]) = 0");
                //        and = " AND ";
                //    }
                //    stringBuilder.AppendLine("\t)");
                //}

                stringBuilder.AppendLine(") THEN");
                stringBuilder.AppendLine("\tUPDATE SET");

                string strSeparator = string.Empty;
                foreach (var mutableColumn in this.tableDescription.GetMutableColumns(false))
                {
                    var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                    stringBuilder.AppendLine($"\t{strSeparator}{columnName} = [changes].{columnName}");
                    strSeparator = ", ";
                }
            }

            stringBuilder.AppendLine("WHEN NOT MATCHED BY TARGET AND ([changes].[sync_timestamp] <= @sync_min_timestamp OR [changes].[sync_timestamp] IS NULL OR @sync_force_write = 1) THEN");


            stringBuilderArguments = new StringBuilder();
            stringBuilderParameters = new StringBuilder();
            var empty = string.Empty;

            foreach (var mutableColumn in this.tableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();

                stringBuilderArguments.Append(string.Concat(empty, columnName));
                stringBuilderParameters.Append(string.Concat(empty, $"[changes].{columnName}"));
                empty = ", ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("\tINSERT");
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()});");
            stringBuilder.AppendLine();

            // GET row count BEFORE make identity insert off again
            stringBuilder.AppendLine($"SET {sqlParameter4.ParameterName} = @@ROWCOUNT;");

            // Check if we have auto inc column
            if (this.tableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {tableName.Schema().Quoted().ToString()} OFF;");
                stringBuilder.AppendLine();
            }

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        //------------------------------------------------------------------
        // Select changes command
        //------------------------------------------------------------------
        protected override SqlCommand BuildSelectInitializedChangesCommand(DbConnection connection, DbTransaction transaction, SyncFilter filter = null)
        {
            var sqlCommand = new SqlCommand();

            var pTimestamp = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt) { Value = "NULL", IsNullable = true };
            sqlCommand.Parameters.Add(pTimestamp);

            // Add filter parameters
            if (filter != null)
                CreateFilterParameters(sqlCommand, filter);

            var stringBuilder = new StringBuilder("");
            stringBuilder.AppendLine(";WITH ");
            stringBuilder.AppendLine($"  {trackingName.Quoted().ToString()} AS (");
            stringBuilder.Append("\tSELECT ");
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.Append($"[CT].{columnName}, ");
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("\tCAST([CT].[SYS_CHANGE_CONTEXT] as uniqueidentifier) AS [sync_update_scope_id], ");
            stringBuilder.AppendLine("\t[CT].[SYS_CHANGE_VERSION] as [sync_timestamp],");
            stringBuilder.AppendLine("\tCASE WHEN [CT].[SYS_CHANGE_OPERATION] = 'D' THEN 1 ELSE 0 END AS [sync_row_is_tombstone]");
            stringBuilder.AppendLine($"\tFROM CHANGETABLE(CHANGES {tableName.Schema().Quoted().ToString()}, @sync_min_timestamp) AS [CT]");
            stringBuilder.AppendLine("\t)");

            stringBuilder.AppendLine("SELECT DISTINCT");

            var columns = this.tableDescription.GetMutableColumns(false, true).ToList();
            for (var i = 0; i < columns.Count; i++)
            {
                var mutableColumn = columns[i];
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilder.Append($"\t[base].{columnName}");

                if (i < columns.Count - 1)
                    stringBuilder.AppendLine(", ");
            }
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"FROM {tableName.Schema().Quoted().ToString()} [base]");
            stringBuilder.Append($"LEFT JOIN {trackingName.Quoted().ToString()} [side]");
            stringBuilder.Append("ON ");

            string empty = string.Empty;
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.Append($"{empty}[base].{columnName} = [side].{columnName}");
                empty = " AND ";
            }

            // ----------------------------------
            // Custom Joins
            // ----------------------------------
            if (filter != null)
                stringBuilder.Append(CreateFilterCustomJoins(filter));

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE (");

            // ----------------------------------
            // Where filters on [side]
            // ----------------------------------
            if (filter != null)
            {
                var createFilterWhereSide = CreateFilterWhereSide(filter, true);
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
                var createFilterCustomWheres = CreateFilterCustomWheres(filter);
                stringBuilder.Append(createFilterCustomWheres);

                if (!string.IsNullOrEmpty(createFilterCustomWheres))
                    stringBuilder.AppendLine("AND ");
            }
            // ----------------------------------

            stringBuilder.AppendLine("\t([side].[sync_timestamp] > @sync_min_timestamp OR @sync_min_timestamp IS NULL)");
            stringBuilder.AppendLine(")");
            sqlCommand.CommandText = stringBuilder.ToString();

            return sqlCommand;
        }

        //------------------------------------------------------------------
        // Select changes command
        //------------------------------------------------------------------
        protected override SqlCommand BuildSelectIncrementalChangesCommand(SyncFilter filter)
        {
            var sqlCommand = new SqlCommand();
            var pTimestamp = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            var pScopeId = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            var setupHasTableWithColumns = setup.HasTableWithColumns(tableDescription.TableName);

            sqlCommand.Parameters.Add(pTimestamp);
            sqlCommand.Parameters.Add(pScopeId);

            // Add filter parameters
            if (filter != null)
                CreateFilterParameters(sqlCommand, filter);

            var stringBuilder = new StringBuilder("");
            stringBuilder.AppendLine(";WITH ");
            stringBuilder.AppendLine($"  {trackingName.Quoted().ToString()} AS (");
            stringBuilder.Append("\tSELECT ");
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.Append($"[CT].{columnName}, ");
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("\tCAST([CT].[SYS_CHANGE_CONTEXT] AS uniqueidentifier) AS [sync_update_scope_id],");
            stringBuilder.AppendLine("\t[CT].[SYS_CHANGE_VERSION] AS [sync_timestamp],");
            stringBuilder.Append("\tCASE WHEN [CT].[SYS_CHANGE_OPERATION] = 'D' THEN 1 ELSE 0 END AS [sync_row_is_tombstone]");
            if (setupHasTableWithColumns)
            {
                stringBuilder.Append(",\n\t[CT].[SYS_CHANGE_COLUMNS] AS [sync_change_columns]");
            }

            stringBuilder.AppendLine($"\n\tFROM CHANGETABLE(CHANGES {tableName.Schema().Quoted().ToString()}, @sync_min_timestamp) AS [CT]");
            stringBuilder.AppendLine("\t)");

            stringBuilder.AppendLine("SELECT DISTINCT");
            //foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            //{
            //    var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
            //    stringBuilder.AppendLine($"\t[side].{columnName},");
            //}
            foreach (var mutableColumn in this.tableDescription.GetMutableColumns(false, true))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();

                var isPrimaryKey = this.tableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.AppendLine($"\t[side].{columnName}, ");
                else
                    stringBuilder.AppendLine($"\t[base].{columnName}, ");
            }
            stringBuilder.AppendLine("\t[side].[sync_row_is_tombstone],");
            stringBuilder.AppendLine("\t[side].[sync_update_scope_id]");
            stringBuilder.AppendLine($"FROM {tableName.Schema().Quoted().ToString()} [base]");
            stringBuilder.Append($"RIGHT JOIN {trackingName.Quoted().ToString()} [side]");
            stringBuilder.Append("ON ");

            string empty = string.Empty;
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.Append($"{empty}[base].{columnName} = [side].{columnName}");
                empty = " AND ";
            }

            // ----------------------------------
            // Custom Joins
            // ----------------------------------
            if (filter != null)
                stringBuilder.Append(CreateFilterCustomJoins(filter));

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE (");

            // ----------------------------------
            // Where filters on [side]
            // ----------------------------------
            if (filter != null)
            {
                var createFilterWhereSide = CreateFilterWhereSide(filter, true);
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
                var createFilterCustomWheres = CreateFilterCustomWheres(filter);
                stringBuilder.Append(createFilterCustomWheres);

                if (!string.IsNullOrEmpty(createFilterCustomWheres))
                    stringBuilder.AppendLine("AND ");
            }
            // ----------------------------------

            stringBuilder.AppendLine("\t[side].[sync_timestamp] > @sync_min_timestamp");
            stringBuilder.AppendLine("\tAND ([side].[sync_update_scope_id] <> @sync_scope_id OR [side].[sync_update_scope_id] IS NULL)");

            //if (setupHasTableWithColumns)
            //{
            //    stringBuilder.AppendLine("\tAND (");
            //    string or = string.Empty;
            //    foreach (var column in this.tableDescription.GetMutableColumns())
            //    {
            //        var unquotedColumn = ParserName.Parse(column).Unquoted().ToString();
            //        stringBuilder.Append("\t\t");
            //        stringBuilder.Append(or);
            //        stringBuilder.Append("CHANGE_TRACKING_IS_COLUMN_IN_MASK(");
            //        stringBuilder.Append($"COLUMNPROPERTY(OBJECT_ID('{tableName.Schema().Quoted().ToString()}'), '{unquotedColumn}', 'ColumnId')");
            //        stringBuilder.AppendLine(", [side].[sync_change_columns]) = 1");
            //        or = " OR ";
            //    }
            //    stringBuilder.AppendLine("\t)");
            //}

            stringBuilder.AppendLine(")");
            sqlCommand.CommandText = stringBuilder.ToString();

            return sqlCommand;
        }

        protected override SqlCommand BuildDeleteMetadataCommand()
        {
            SqlCommand sqlCommand = new SqlCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);
            SqlParameter sqlParameter1 = new SqlParameter("@sync_row_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter1);
            SqlParameter sqlParameter2 = new SqlParameter("@sync_row_count", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output
            };
            sqlCommand.Parameters.Add(sqlParameter2);
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"SET {sqlParameter2.ParameterName} = 0;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("SELECT 1;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("SET ", sqlParameter2.ParameterName, " = 1;"));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        protected override SqlCommand BuildResetCommand()
        {
            SqlCommand sqlCommand = new SqlCommand();
            SqlParameter sqlParameter2 = new SqlParameter("@sync_row_count", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output
            };
            sqlCommand.Parameters.Add(sqlParameter2);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"SET {sqlParameter2.ParameterName} = 0;");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"ALTER TABLE {tableName.Schema().Quoted().ToString()} DISABLE CHANGE_TRACKING;");
            stringBuilder.AppendLine($"DELETE FROM {tableName.Schema().Quoted().ToString()};");

            if (setup.HasTableWithColumns(tableDescription.TableName))
            {
                stringBuilder.AppendLine($"ALTER TABLE {tableName.Schema().Quoted().ToString()} ENABLE CHANGE_TRACKING WITH(TRACK_COLUMNS_UPDATED = ON);");
            }
            else
            {
                stringBuilder.AppendLine($"ALTER TABLE {tableName.Schema().Quoted().ToString()} ENABLE CHANGE_TRACKING WITH(TRACK_COLUMNS_UPDATED = OFF);");
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("SET ", sqlParameter2.ParameterName, " = @@ROWCOUNT;"));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
    }
}
