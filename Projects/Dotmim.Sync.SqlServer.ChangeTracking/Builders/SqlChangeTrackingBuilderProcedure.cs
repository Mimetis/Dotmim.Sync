using Dotmim.Sync.Builders;
using System;
using System.Text;

using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Data;
using Dotmim.Sync.Log;
using System.Linq;

using Dotmim.Sync.SqlServer.Manager;
using System.Diagnostics;
using System.Collections.Generic;
using Dotmim.Sync.SqlServer.Builders;

namespace Dotmim.Sync.SqlServer.ChangeTracking.Builders
{
    public class SqlChangeTrackingBuilderProcedure : SqlBuilderProcedure
    {
        private ParserName tableName;
        private ParserName trackingName;
        private SqlConnection connection;
        private readonly SqlTransaction transaction;
        private readonly SyncTable tableDescription;
        private readonly SyncSetup setup;
        private readonly SqlObjectNames sqlObjectNames;
        private readonly SqlDbMetadata sqlDbMetadata;

        public SqlChangeTrackingBuilderProcedure(SyncTable tableDescription, SyncSetup setup, DbConnection connection, DbTransaction transaction = null)
            : base(tableDescription, setup, connection, transaction)
        {
            this.connection = connection as SqlConnection;
            this.transaction = transaction as SqlTransaction;

            this.tableDescription = tableDescription;
            this.setup = setup;
            (this.tableName, this.trackingName) = SqlTableBuilder.GetParsers(tableDescription, setup);
            this.sqlObjectNames = new SqlObjectNames(this.tableDescription, this.setup);
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
                TypeName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkTableType).name
            };
            sqlCommand.Parameters.Add(sqlParameter2);


            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[p]", "[CT]");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[changes]", "[base]");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[t]", "[side]");
            string str7 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[p]", "[side]");

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got deleted");
            stringBuilder.Append("declare @changed TABLE (");
            foreach (var c in this.tableDescription.GetPrimaryKeysColumns())
            {
                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = this.sqlDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.GetDbType(), false, false, c.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var quotedColumnType = ParserName.Parse(sqlDbTypeString).Quoted().ToString();

                quotedColumnType += this.sqlDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.GetDbType(), false, false, c.MaxLength, c.Precision, c.Scale, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

                stringBuilder.Append($"{ParserName.Parse(c).Quoted().ToString()} {quotedColumnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < this.tableDescription.PrimaryKeys.Count; i++)
            {
                var cc = ParserName.Parse(this.tableDescription.PrimaryKeys[i]).Quoted().ToString();
                stringBuilder.Append($"{cc}");

                if (i < this.tableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append(", ");
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
            stringBuilder.AppendLine($"\tCAST([CT].[SYS_CHANGE_CONTEXT] as uniqueidentifier) AS [update_scope_id], ");
            stringBuilder.AppendLine($"\t[CT].[SYS_CHANGE_VERSION] as [timestamp],");
            stringBuilder.AppendLine($"\tCASE WHEN [CT].[SYS_CHANGE_OPERATION] = 'D' THEN 1 ELSE 0 END AS [sync_row_is_tombstone]");
            stringBuilder.AppendLine($"\tFROM @changeTable AS [p] ");
            stringBuilder.AppendLine($"\tLEFT JOIN CHANGETABLE(CHANGES {tableName.Schema().Quoted().ToString()}, @sync_min_timestamp) AS [CT] ON {str4}");
            stringBuilder.AppendLine($"\t)");


            stringBuilder.AppendLine($"DELETE {tableName.Schema().Quoted().ToString()}");
            stringBuilder.Append($"OUTPUT ");
            for (int i = 0; i < this.tableDescription.PrimaryKeys.Count; i++)
            {
                var cc = ParserName.Parse(this.tableDescription.PrimaryKeys[i]).Quoted().ToString();
                stringBuilder.Append($"DELETED.{cc}");
                if (i < this.tableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine($"INTO @changed ");
            stringBuilder.AppendLine($"FROM {tableName.Quoted().ToString()} [base]");
            stringBuilder.AppendLine($"JOIN {trackingName.Quoted().ToString()} [changes] ON {str5}");
            stringBuilder.AppendLine("WHERE [changes].[timestamp] <= @sync_min_timestamp OR [changes].[timestamp] IS NULL OR [changes].[update_scope_id] = @sync_scope_id;");
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
            string empty = string.Empty;

            var sqlParameter = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter);

            var sqlParameter1 = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter1);

            var sqlParameter2 = new SqlParameter("@changeTable", SqlDbType.Structured)
            {
                TypeName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkTableType).name
            };
            sqlCommand.Parameters.Add(sqlParameter2);

            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[CT]", "[p]");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[changes]", "[base]");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[t]", "[side]");
            string str7 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[p]", "[side]");

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @changed TABLE (");
            foreach (var c in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();

                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = this.sqlDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.GetDbType(), false, false, c.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var quotedColumnType = ParserName.Parse(sqlDbTypeString).Quoted().ToString();
                quotedColumnType += this.sqlDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.GetDbType(), false, false, c.MaxLength, c.Precision, c.Scale, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

                stringBuilder.Append($"{columnName} {quotedColumnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < this.tableDescription.PrimaryKeys.Count; i++)
            {
                var columnName = ParserName.Parse(this.tableDescription.PrimaryKeys[i]).Quoted().ToString();
                stringBuilder.Append($"{columnName}");
                if (i < this.tableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append(", ");
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


            stringBuilder.AppendLine($"DECLARE @var_sync_scope_id varbinary(128) = cast(@sync_scope_id as varbinary(128));");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($";WITH ");
            stringBuilder.AppendLine($"  CHANGE_TRACKING_CONTEXT(@var_sync_scope_id),");
            stringBuilder.AppendLine($"  {trackingName.Quoted().ToString()} AS (");
            stringBuilder.Append($"\tSELECT ");
            foreach (var c in this.tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"[p].{columnName}, ");
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\tCAST([CT].[SYS_CHANGE_CONTEXT] as uniqueidentifier) AS [update_scope_id], ");
            stringBuilder.AppendLine($"\t[CT].[SYS_CHANGE_VERSION] as [timestamp],");
            stringBuilder.AppendLine($"\tCASE WHEN [CT].[SYS_CHANGE_OPERATION] = 'D' THEN 1 ELSE 0 END AS [sync_row_is_tombstone]");
            stringBuilder.AppendLine($"\tFROM @changeTable AS [p] ");
            stringBuilder.AppendLine($"\tLEFT JOIN CHANGETABLE(CHANGES {tableName.Schema().Quoted().ToString()}, @sync_min_timestamp) AS [CT] ON {str4}");
            stringBuilder.AppendLine($"\t)");
            stringBuilder.AppendLine($"MERGE {tableName.Schema().Quoted().ToString()} AS [base]");
            stringBuilder.AppendLine($"USING {trackingName.Quoted().ToString()} as [changes] ON {str5}");

            if (hasMutableColumns)
            {
                stringBuilder.AppendLine("WHEN MATCHED AND ([changes].[timestamp] <= @sync_min_timestamp OR [changes].[timestamp] IS NULL OR [changes].[update_scope_id] = @sync_scope_id) THEN");
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"\tUPDATE SET");

                string strSeparator = "";
                foreach (var mutableColumn in this.tableDescription.GetMutableColumns(false))
                {
                    var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                    stringBuilder.AppendLine($"\t{strSeparator}{columnName} = [changes].{columnName}");
                    strSeparator = ", ";
                }
            }

            stringBuilder.AppendLine("WHEN NOT MATCHED BY TARGET AND ([changes].[timestamp] <= @sync_min_timestamp OR [changes].[timestamp] IS NULL) THEN");


            stringBuilderArguments = new StringBuilder();
            stringBuilderParameters = new StringBuilder();
            empty = string.Empty;

            foreach (var mutableColumn in this.tableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();

                stringBuilderArguments.Append(string.Concat(empty, columnName));
                stringBuilderParameters.Append(string.Concat(empty, $"[changes].{columnName}"));
                empty = ", ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\tINSERT");
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()})");
            stringBuilder.AppendLine();
            stringBuilder.Append($"OUTPUT ");
            for (int i = 0; i < this.tableDescription.PrimaryKeys.Count; i++)
            {
                var columnName = ParserName.Parse(this.tableDescription.PrimaryKeys[i]).Quoted().ToString();
                stringBuilder.Append($"INSERTED.{columnName}");
                if (i < this.tableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine($"\tINTO @changed; -- populates the temp table with successful PKs");
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
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[changes]", "[base]");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[t]", "[side]");
            string str7 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[p]", "[side]");


            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"SET {sqlParameter2.ParameterName} = 0;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"DECLARE @var_sync_scope_id varbinary(128) = cast(@sync_scope_id as varbinary(128));");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($";WITH ");
            stringBuilder.AppendLine($"  CHANGE_TRACKING_CONTEXT(@var_sync_scope_id),");
            stringBuilder.AppendLine($"  {trackingName.Quoted().ToString()} AS (");
            stringBuilder.Append("\tSELECT ");
            foreach (var c in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"[p].{columnName}, ");
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\tCAST([CT].[SYS_CHANGE_CONTEXT] as uniqueidentifier) AS [update_scope_id], ");
            stringBuilder.AppendLine($"\t[CT].[SYS_CHANGE_VERSION] as [timestamp],");
            stringBuilder.AppendLine($"\tCASE WHEN [CT].[SYS_CHANGE_OPERATION] = 'D' THEN 1 ELSE 0 END AS [sync_row_is_tombstone]");
            stringBuilder.Append($"\tFROM (SELECT ");
            string comma = "";
            foreach (var c in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                var columnParameterName = ParserName.Parse(c).Unquoted().Normalized().ToString();

                stringBuilder.Append($"{comma}@{columnParameterName} as {columnName}");
                comma = ", ";
            }
            stringBuilder.AppendLine($") AS [p]");
            stringBuilder.Append($"\tLEFT JOIN CHANGETABLE(CHANGES {tableName.Schema().Quoted().ToString()}, @sync_min_timestamp) AS [CT] ON {str4}");
            stringBuilder.AppendLine($"\t)");
            stringBuilder.AppendLine($"DELETE {tableName.Schema().Quoted().ToString()}");
            stringBuilder.AppendLine($"FROM {tableName.Schema().Quoted().ToString()} [base]");
            stringBuilder.Append($"JOIN {trackingName.Quoted().ToString()} [side] ON ");

            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[base]", "[side]"));

            stringBuilder.AppendLine("WHERE ([side].[timestamp] <= @sync_min_timestamp OR [side].[timestamp] IS NULL OR [side].[update_scope_id] = @sync_scope_id OR @sync_force_write = 1)");
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
            var stringBuilder2 = new StringBuilder();
            var stringBuilder22 = new StringBuilder();
            var stringBuilder3 = new StringBuilder();
            var stringBuilder4 = new StringBuilder();

            string empty = string.Empty;
            string comma = string.Empty;
            foreach (var pkColumn in this.tableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();

                stringBuilder1.Append($"{empty}[side].{columnName} = @{parameterName}");
                stringBuilder11.Append($"{empty}[base].{columnName} = @{parameterName}");
                stringBuilder2.AppendLine($"\t[side].{columnName}, ");
                stringBuilder22.AppendLine($"\t[base].{columnName}, ");
                stringBuilder3.Append($"{comma}{columnName}");
                stringBuilder4.Append($"{empty}[base].{columnName} = [side].{columnName}");

                empty = " AND ";
                comma = ", ";
            }

            var stringBuilderColumns = new StringBuilder();
            foreach (var mutableColumn in this.tableDescription.GetMutableColumns())
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilderColumns.AppendLine($"\t[base].{columnName}, ");
            }

            var stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"IF (SELECT TOP 1 1 FROM CHANGETABLE(CHANGES {tableName.Schema().Quoted().ToString()}, 0) AS [side] WHERE ({stringBuilder1.ToString()})) > 0");
            stringBuilder.AppendLine($"BEGIN");
            stringBuilder.AppendLine($"\tSELECT");
            // add side pkeys
            stringBuilder.Append(stringBuilder2.ToString());
            // add columns
            stringBuilder.Append(stringBuilderColumns.ToString());
            stringBuilder.AppendLine($"\tCAST([side].SYS_CHANGE_CONTEXT as uniqueidentifier) AS [update_scope_id],");
            stringBuilder.AppendLine($"\tCASE [side].SYS_CHANGE_OPERATION WHEN 'D' THEN 1 ELSE 0 END AS [sync_row_is_tombstone]");
            stringBuilder.AppendLine($"\tFROM CHANGETABLE(CHANGES {tableName.Schema().Quoted().ToString()}, 0) AS [side]");
            stringBuilder.AppendLine($"\tLEFT JOIN {tableName.Schema().Quoted().ToString()} [base] ON");
            stringBuilder.AppendLine($"\t\t{stringBuilder4.ToString()}");
            stringBuilder.AppendLine($"\tWHERE {stringBuilder1.ToString()}");
            stringBuilder.AppendLine($"END");
            stringBuilder.AppendLine($"ELSE");
            stringBuilder.AppendLine($"BEGIN");
            stringBuilder.AppendLine($"\tSELECT");
            // add base pkeys
            stringBuilder.Append(stringBuilder22.ToString());
            // add base columns
            stringBuilder.Append(stringBuilderColumns.ToString());
            stringBuilder.AppendLine($"\tnull as update_scope_id, ");
            stringBuilder.AppendLine($"\t0 as sync_row_is_tombstone ");
            stringBuilder.AppendLine($"\tFROM {tableName.Schema().Quoted().ToString()} as [base] ");
            stringBuilder.Append(string.Concat("\tWHERE ", stringBuilder11.ToString()));
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"END");



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
            stringBuilder.AppendLine($"DECLARE @var_sync_scope_id varbinary(128) = cast(@sync_scope_id as varbinary(128));");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"SET {sqlParameter4.ParameterName} = 0;");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($";WITH ");
            stringBuilder.AppendLine($"  CHANGE_TRACKING_CONTEXT(@var_sync_scope_id),");
            stringBuilder.AppendLine($"  {trackingName.Quoted().ToString()} AS (");
            stringBuilder.Append("\tSELECT ");
            foreach (var c in this.tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"[p].{columnName}, ");
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\tCAST([CT].[SYS_CHANGE_CONTEXT] as uniqueidentifier) AS [update_scope_id], ");
            stringBuilder.AppendLine($"\t[CT].[SYS_CHANGE_VERSION] as [timestamp],");
            stringBuilder.AppendLine($"\tCASE WHEN [CT].[SYS_CHANGE_OPERATION] = 'D' THEN 1 ELSE 0 END AS [sync_row_is_tombstone]");
            stringBuilder.AppendLine($"\tFROM (SELECT ");
            stringBuilder.Append($"\t\t ");
            string comma = "";
            foreach (var c in this.tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                var columnParameterName = ParserName.Parse(c).Unquoted().Normalized().ToString();

                stringBuilder.Append($"{comma}@{columnParameterName} as {columnName}");
                comma = ", ";
            }
            stringBuilder.AppendLine($") AS [p]");
            stringBuilder.Append($"\tLEFT JOIN CHANGETABLE(CHANGES {tableName.Schema().Quoted().ToString()}, @sync_min_timestamp) AS [CT] ON {str4}");
            stringBuilder.AppendLine($"\t)");

            stringBuilder.AppendLine($"MERGE {tableName.Schema().Quoted().ToString()} AS [base]");
            stringBuilder.AppendLine($"USING {trackingName.Quoted().ToString()} as [changes] ON {str5}");

            if (hasMutableColumns)
            {
                stringBuilder.AppendLine("WHEN MATCHED AND ([changes].[timestamp] <= @sync_min_timestamp OR [changes].[timestamp] IS NULL OR [changes].[update_scope_id] = @sync_scope_id OR @sync_force_write = 1) THEN");
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"\tUPDATE SET");

                string strSeparator = "";
                foreach (var mutableColumn in this.tableDescription.GetMutableColumns(false))
                {
                    var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                    stringBuilder.AppendLine($"\t{strSeparator}{columnName} = [changes].{columnName}");
                    strSeparator = ", ";
                }
            }

            stringBuilder.AppendLine("WHEN NOT MATCHED BY TARGET AND ([changes].[timestamp] <= @sync_min_timestamp OR [changes].[timestamp] IS NULL OR @sync_force_write = 1) THEN");


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
            stringBuilder.AppendLine($"\tINSERT");
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
        protected override SqlCommand BuildSelectIncrementalChangesCommand(SyncFilter filter)
        {
            var sqlCommand = new SqlCommand();
            var pTimestamp = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            var pScopeId = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);



            sqlCommand.Parameters.Add(pTimestamp);
            sqlCommand.Parameters.Add(pScopeId);

            // Add filter parameters
            if (filter != null)
                CreateFilterParameters(sqlCommand, filter);

            var stringBuilder = new StringBuilder("");
            stringBuilder.AppendLine($";WITH ");
            stringBuilder.AppendLine($"  {trackingName.Quoted().ToString()} AS (");
            stringBuilder.Append($"\tSELECT ");
            foreach (var pkColumn in this.tableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.Append($"[CT].{columnName}, ");
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\tCAST([CT].[SYS_CHANGE_CONTEXT] as uniqueidentifier) AS [update_scope_id], ");
            stringBuilder.AppendLine($"\t[CT].[SYS_CHANGE_VERSION] as [timestamp],");
            stringBuilder.AppendLine($"\tCASE WHEN [CT].[SYS_CHANGE_OPERATION] = 'D' THEN 1 ELSE 0 END AS [sync_row_is_tombstone]");
            stringBuilder.AppendLine($"\tFROM CHANGETABLE(CHANGES {tableName.Schema().Quoted().ToString()}, @sync_min_timestamp) AS [CT]");
            stringBuilder.AppendLine($"\t)");

            stringBuilder.AppendLine("SELECT DISTINCT");
            foreach (var pkColumn in this.tableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.AppendLine($"\t[side].{columnName}, ");
            }
            foreach (var mutableColumn in this.tableDescription.GetMutableColumns())
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilder.AppendLine($"\t[base].{columnName}, ");
            }
            stringBuilder.AppendLine($"\t[side].[sync_row_is_tombstone], ");
            stringBuilder.AppendLine($"\t[side].[update_scope_id] ");
            stringBuilder.AppendLine($"FROM {tableName.Schema().Quoted().ToString()} [base]");
            stringBuilder.Append($"RIGHT JOIN {trackingName.Quoted().ToString()} [side]");
            stringBuilder.Append($"ON ");

            string empty = "";
            foreach (var pkColumn in this.tableDescription.PrimaryKeys)
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
            string str = string.Empty;

            // ----------------------------------
            // Where filters on [side]
            // ----------------------------------
            if (filter != null)
            {
                var createFilterWhereSide = CreateFilterWhereSide(filter, true);
                stringBuilder.Append(createFilterWhereSide);

                if (!string.IsNullOrEmpty(createFilterWhereSide))
                    stringBuilder.AppendLine($"AND ");
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
                    stringBuilder.AppendLine($"AND ");
            }
            // ----------------------------------



            stringBuilder.AppendLine("\t[side].[timestamp] > @sync_min_timestamp");
            stringBuilder.AppendLine("\tAND ([side].[update_scope_id] <> @sync_scope_id OR [side].[update_scope_id] IS NULL)");
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
            stringBuilder.AppendLine($"SELECT 1;");
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

            stringBuilder.AppendLine($"ALTER TABLE {tableName.Schema().Quoted()} DISABLE CHANGE_TRACKING;");
            stringBuilder.AppendLine($"DELETE FROM {tableName.Schema().Quoted()};");
            stringBuilder.AppendLine($"ALTER TABLE {tableName.Schema().Quoted()} ENABLE CHANGE_TRACKING WITH(TRACK_COLUMNS_UPDATED = OFF);");

            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("SET ", sqlParameter2.ParameterName, " = @@ROWCOUNT;"));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
    }
}