using Dotmim.Sync.Builders;
using System;
using System.Text;
using Dotmim.Sync.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data;
using Dotmim.Sync.Log;
using System.Linq;
using Dotmim.Sync.Filter;
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
        private readonly SqlObjectNames sqlObjectNames;
        private readonly SqlDbMetadata sqlDbMetadata;

        public SqlChangeTrackingBuilderProcedure(SyncTable tableDescription, DbConnection connection, DbTransaction transaction = null)
            : base(tableDescription, connection, transaction)
        {
            this.connection = connection as SqlConnection;
            this.transaction = transaction as SqlTransaction;

            this.tableDescription = tableDescription;
            (this.tableName, this.trackingName) = SqlBuilder.GetParsers(tableDescription);
            this.sqlObjectNames = new SqlObjectNames(this.tableDescription);
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
            var sqlParameter2 = new SqlParameter("@changeTable", SqlDbType.Structured)
            {
                TypeName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkTableType).name
            };
            sqlCommand.Parameters.Add(sqlParameter2);


            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[p]", "[t]");
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
            stringBuilder.AppendLine("JOIN (");


            stringBuilder.AppendLine($"\tSELECT ");
            string str = "";
            stringBuilder.Append("\t");
            foreach (var c in this.tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"{str}[p].{columnName}");
                str = ", ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("\t, [t].[update_scope_id], [t].[sync_row_is_frozen], [t].[timestamp]");
            stringBuilder.AppendLine($"\tFROM @changeTable p ");
            stringBuilder.AppendLine($"\tJOIN {trackingName.Schema().Quoted().ToString()} t ON ");
            stringBuilder.AppendLine($"\t{str4}");
            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine("\tAS [changes] ON ");
            stringBuilder.AppendLine(str5);
            stringBuilder.AppendLine("WHERE [changes].[sync_row_is_frozen] = 1 OR [changes].[timestamp] <= @sync_min_timestamp;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- Since the delete trigger is passed, we update the tracking table to reflect the real scope deleter");
            stringBuilder.AppendLine("UPDATE [side] SET");
            stringBuilder.AppendLine("\tsync_row_is_tombstone = 1, ");
            stringBuilder.AppendLine("\tsync_row_is_frozen = 1, ");
            stringBuilder.AppendLine("\tupdate_scope_id = [p].[update_scope_id],");
            stringBuilder.AppendLine("\tlast_change_datetime = GETUTCDATE()");
            stringBuilder.AppendLine($"FROM {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.AppendLine($"JOIN @changed [t] on {str6}");
            stringBuilder.AppendLine($"JOIN @changeTable [p] on {str7}");
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

            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[p]", "[t]");
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


            stringBuilder.AppendLine($"DECLARE @originator_id varbinary(128);");
            stringBuilder.AppendLine($"Declare @jsonval char(128) = '{{\"sync_row_is_frozen\":1,\"update_scope_id\":\"' + cast(@sync_scope_id as char(36)) + '\"}}';");
            stringBuilder.AppendLine($"SET @originator_id = CAST(@jsonval AS varbinary(128));");
            stringBuilder.AppendLine();
            //stringBuilder.AppendLine($"With CHANGE_TRACKING_CONTEXT(@originator_id)");

            stringBuilder.AppendLine($";WITH ");
            stringBuilder.AppendLine($"  CHANGE_TRACKING_CONTEXT(@originator_id),");
            stringBuilder.AppendLine($"  {trackingName.Schema().Quoted().ToString()} AS (");
            stringBuilder.AppendLine($"\tSELECT CT.*, ");
            stringBuilder.AppendLine($"\tCASE");
            stringBuilder.AppendLine($"\t\tWHEN JSON_VALUE(CAST(CT.SYS_CHANGE_CONTEXT as char(128)), '$.sync_row_is_frozen') is null then 0");
            stringBuilder.AppendLine($"\t\tELSE 1");
            stringBuilder.AppendLine($"\tEND AS sync_row_is_frozen,");
            stringBuilder.AppendLine($"\tCAST(JSON_VALUE(CAST(CT.SYS_CHANGE_CONTEXT as char(128)), '$.update_scope_id') as uniqueidentifier) as update_scope_id,");
            stringBuilder.AppendLine($"\tCT.sys_change_version as [timestamp],");
            stringBuilder.AppendLine($"\tCASE [CT].SYS_CHANGE_OPERATION WHEN 'D' THEN 1 ELSE 0 END AS sync_row_is_tombstone");
            stringBuilder.AppendLine($"\tFROM CHANGETABLE(CHANGES {tableName.Schema().Quoted().ToString()}, @sync_min_timestamp) as CT");
            stringBuilder.AppendLine($")");
            stringBuilder.AppendLine("-- update the base table");
            stringBuilder.AppendLine($"MERGE {tableName.Schema().Quoted().ToString()} AS base USING");
            stringBuilder.AppendLine("\t-- join done here against the side table to get the local timestamp for concurrency check\n");

            stringBuilder.AppendLine($"\t(SELECT ");
            string str = "";
            stringBuilder.Append("\t");
            foreach (var c in this.tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"{str}[p].{columnName}");
                str = ", ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("\t, [t].[update_scope_id], [t].[sync_row_is_frozen], [t].[timestamp]");
            stringBuilder.AppendLine($"\tFROM @changeTable p ");
            stringBuilder.Append($"\tLEFT JOIN {trackingName.Schema().Quoted().ToString()} t ON ");
            stringBuilder.AppendLine($" {str4}");
            stringBuilder.AppendLine($"\t) AS changes ON {str5}");
            stringBuilder.AppendLine();
            if (hasMutableColumns)
            {
                stringBuilder.AppendLine("WHEN MATCHED AND ([changes].[timestamp] <= @sync_min_timestamp OR [changes].[sync_row_is_frozen] = 1) THEN");
                foreach (var mutableColumn in this.tableDescription.Columns.Where(c => !c.IsReadOnly))
                {
                    var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                    stringBuilderArguments.Append(string.Concat(empty, columnName));
                    stringBuilderParameters.Append(string.Concat(empty, $"changes.{columnName}"));
                    empty = ", ";
                }
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

            stringBuilder.AppendLine("WHEN NOT MATCHED BY TARGET AND ([changes].[timestamp] <= @sync_min_timestamp OR changes.[timestamp] IS NULL) THEN");


            stringBuilderArguments = new StringBuilder();
            stringBuilderParameters = new StringBuilder();
            empty = string.Empty;

            foreach (var mutableColumn in this.tableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();

                stringBuilderArguments.Append(string.Concat(empty, columnName));
                stringBuilderParameters.Append(string.Concat(empty, $"changes.{columnName}"));
                empty = ", ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\tINSERT");
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()})");


            stringBuilder.Append($"\tOUTPUT ");
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
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {tableName.Schema().Quoted().ToString()} ON;");
                stringBuilder.AppendLine();
            }

            //stringBuilder.AppendLine("-- Since the update trigger is passed, we update the tracking table to reflect the real scope updater");
            //stringBuilder.AppendLine("UPDATE side SET");
            //stringBuilder.AppendLine("\tupdate_scope_id = [p].[update_scope_id],");
            //stringBuilder.AppendLine("\tsync_row_is_tombstone = 0,");
            //stringBuilder.AppendLine("\tsync_row_is_frozen = 1,");
            //stringBuilder.AppendLine("\tlast_change_datetime = GETUTCDATE()");
            //stringBuilder.AppendLine($"FROM {trackingName.Schema().Quoted().ToString()} [side]");
            //stringBuilder.AppendLine($"JOIN @changed [t] on {str6}");
            //stringBuilder.AppendLine($"JOIN @changeTable [p] on {str7}");
            //stringBuilder.AppendLine();

            stringBuilder.Append(BulkSelectUnsuccessfulRows());

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }


        //------------------------------------------------------------------
        // Delete command
        //------------------------------------------------------------------
        protected override SqlCommand BuildDeleteCommand()
        {
            SqlCommand sqlCommand = new SqlCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);
            SqlParameter sqlParameter = new SqlParameter("@sync_force_write", SqlDbType.Int);
            sqlCommand.Parameters.Add(sqlParameter);
            SqlParameter sqlParameter1 = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter1);
            SqlParameter sqlParameter2 = new SqlParameter("@sync_row_count", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output
            };
            sqlCommand.Parameters.Add(sqlParameter2);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"SET {sqlParameter2.ParameterName} = 0;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"DELETE {tableName.Schema().Quoted().ToString()}");
            stringBuilder.AppendLine($"FROM {tableName.Schema().Quoted().ToString()} [base]");
            stringBuilder.AppendLine($"JOIN {trackingName.Schema().Quoted().ToString()} [side] ON ");

            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[base]", "[side]"));

            stringBuilder.AppendLine("WHERE ([side].[timestamp] <= @sync_min_timestamp OR [side].[sync_row_is_frozen] = 1 OR @sync_force_write = 1)");
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
            stringBuilder.AppendLine($"\tCAST(JSON_VALUE(CAST([side].SYS_CHANGE_CONTEXT as char(128)), '$.update_scope_id') AS uniqueidentifier) AS update_scope_id,");
            stringBuilder.AppendLine($"\tCASE [side].SYS_CHANGE_OPERATION WHEN 'D' THEN 1 ELSE 0 END AS sync_row_is_tombstone");
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
            var stringBuilder = new StringBuilder();

            this.AddColumnParametersToCommand(sqlCommand);

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

            stringBuilder.AppendLine($"SET {sqlParameter4.ParameterName} = 0;");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"DECLARE @originator_id varbinary(128);");
            stringBuilder.AppendLine($"Declare @jsonval char(128) = '{{\"sync_row_is_frozen\":1,\"update_scope_id\":\"' + cast(@sync_scope_id as char(36)) + '\"}}';");
            stringBuilder.AppendLine($"SET @originator_id = CAST(@jsonval AS varbinary(128));");
            stringBuilder.AppendLine();

            if (hasMutableColumns)
            {
                stringBuilder.AppendLine($";WITH ");
                stringBuilder.AppendLine($"  CHANGE_TRACKING_CONTEXT(@originator_id),");
                stringBuilder.AppendLine($"  {trackingName.Schema().Quoted().ToString()} AS (");
                stringBuilder.AppendLine($"\tSELECT CT.*, ");
                stringBuilder.AppendLine($"\tCASE");
                stringBuilder.AppendLine($"\t\tWHEN JSON_VALUE(CAST(CT.SYS_CHANGE_CONTEXT as char(128)), '$.sync_row_is_frozen') is null then 0");
                stringBuilder.AppendLine($"\t\tELSE 1");
                stringBuilder.AppendLine($"\tEND AS sync_row_is_frozen,");
                stringBuilder.AppendLine($"\tCAST(JSON_VALUE(CAST(CT.SYS_CHANGE_CONTEXT as char(128)), '$.update_scope_id') as uniqueidentifier) as update_scope_id,");
                stringBuilder.AppendLine($"\tCASE WHEN [CT].SYS_CHANGE_VERSION IS NULL THEN 0 ELSE [CT].SYS_CHANGE_VERSION END AS [timestamp],");
                stringBuilder.AppendLine($"\tCASE [CT].SYS_CHANGE_OPERATION WHEN 'D' THEN 1 ELSE 0 END AS sync_row_is_tombstone");
                stringBuilder.AppendLine($"\tFrom CHANGETABLE(CHANGES {tableName.Schema().Quoted().ToString()}, @sync_min_timestamp) as CT");
                stringBuilder.AppendLine($"\t)");
                stringBuilder.AppendLine($"\tUPDATE {tableName.Schema().Quoted().ToString()}");
                stringBuilder.Append($"\tSET {SqlManagementUtils.CommaSeparatedUpdateFromParameters(this.tableDescription)}");
                stringBuilder.AppendLine($"\tFROM {tableName.Schema().Quoted().ToString()} [base]");
                stringBuilder.AppendLine($"\tJOIN {trackingName.Schema().Quoted().ToString()} [side]");
                stringBuilder.Append($"\tON ");
                stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[base]", "[side]"));
                stringBuilder.AppendLine("\tWHERE ([side].[timestamp] <= @sync_min_timestamp OR [side].[sync_row_is_frozen] = 1 OR @sync_force_write = 1)");
                stringBuilder.Append("\tAND (");
                stringBuilder.Append(SqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKeys, "[base]"));
                stringBuilder.AppendLine(");");
                stringBuilder.AppendLine();

                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET {sqlParameter4.ParameterName} = @@ROWCOUNT;");
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"IF ({sqlParameter4.ParameterName} = 0)");
                stringBuilder.AppendLine($"BEGIN");

            }

            if (this.tableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine($"\tSET IDENTITY_INSERT {tableName.Schema().Quoted().ToString()} ON;");
                stringBuilder.AppendLine();
            }

            string empty = string.Empty;
            foreach (var mutableColumn in this.tableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                var parameterName = ParserName.Parse(mutableColumn).Unquoted().Normalized().ToString();
                stringBuilderArguments.Append(string.Concat(empty, columnName));
                stringBuilderParameters.Append(string.Concat(empty, $"@{parameterName}"));
                empty = ", ";
            }
            stringBuilder.AppendLine($";WITH ");
            stringBuilder.AppendLine($"  CHANGE_TRACKING_CONTEXT(@originator_id),");
            stringBuilder.AppendLine($"  {trackingName.Schema().Quoted().ToString()} AS (");
            stringBuilder.AppendLine($"\tSELECT CT.*, ");
            stringBuilder.AppendLine($"\tCASE");
            stringBuilder.AppendLine($"\t\tWHEN JSON_VALUE(CAST(CT.SYS_CHANGE_CONTEXT as char(128)), '$.sync_row_is_frozen') is null then 0");
            stringBuilder.AppendLine($"\t\tELSE 1");
            stringBuilder.AppendLine($"\tEND AS sync_row_is_frozen,");
            stringBuilder.AppendLine($"\tCAST(JSON_VALUE(CAST(CT.SYS_CHANGE_CONTEXT as char(128)), '$.update_scope_id') as uniqueidentifier) as update_scope_id,");
            stringBuilder.AppendLine($"\tCASE WHEN [CT].SYS_CHANGE_VERSION IS NULL THEN 0 ELSE [CT].SYS_CHANGE_VERSION END AS [timestamp],");
            stringBuilder.AppendLine($"\tCASE [CT].SYS_CHANGE_OPERATION WHEN 'D' THEN 1 ELSE 0 END AS sync_row_is_tombstone");
            stringBuilder.AppendLine($"From CHANGETABLE(CHANGES {tableName.Schema().Quoted().ToString()}, @sync_min_timestamp) as CT");
            stringBuilder.AppendLine($")");
            stringBuilder.AppendLine($"\tINSERT INTO {tableName.Schema().Quoted().ToString()}");
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"\tSELECT {stringBuilderParameters.ToString()}");
            stringBuilder.AppendLine($"\tWHERE (");
            stringBuilder.AppendLine($"\t EXISTS (SELECT 1 FROM {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.AppendLine($"\t    WHERE ({SqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKeys, "[side]")}) ");
            stringBuilder.AppendLine($"\t    AND ([side].[timestamp] <= @sync_min_timestamp or [side].[sync_row_is_frozen] = 1))");
            stringBuilder.AppendLine($"\t OR NOT EXISTS (SELECT 1 FROM {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.AppendLine($"\t    WHERE ({SqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKeys, "[side]")})) ");
            stringBuilder.AppendLine($"\t OR @sync_force_write = 1");
            stringBuilder.AppendLine($"\t )");

            stringBuilder.AppendLine();

            if (this.tableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"\tSET IDENTITY_INSERT {tableName.Schema().Quoted().ToString()} OFF;");
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\tSET {sqlParameter4.ParameterName} = @@ROWCOUNT;");

            if (hasMutableColumns)
                stringBuilder.AppendLine($"END");

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        //------------------------------------------------------------------
        // Update Metadata command
        //------------------------------------------------------------------
        protected override SqlCommand BuildUpdateMetadataCommand()
        {
            var sqlCommand = new SqlCommand();
            var stringBuilder = new StringBuilder();
            this.AddPkColumnParametersToCommand(sqlCommand);
            var sqlParameter = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter);
            var sqlParameter1 = new SqlParameter("@sync_row_is_tombstone", SqlDbType.Int);
            sqlCommand.Parameters.Add(sqlParameter1);
            var sqlParameter2 = new SqlParameter("@sync_row_is_frozen", SqlDbType.Int);
            sqlCommand.Parameters.Add(sqlParameter2);
            var sqlParameter3 = new SqlParameter("@create_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter3);
            var sqlParameter5 = new SqlParameter("@update_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter5);
            var sqlParameter8 = new SqlParameter("@sync_row_count", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output
            };
            sqlCommand.Parameters.Add(sqlParameter8);

            stringBuilder.AppendLine($"SET {sqlParameter8.ParameterName} = 0;");
            stringBuilder.AppendLine($"SELECT 1;");

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        //------------------------------------------------------------------
        // Select changes command
        //------------------------------------------------------------------
        protected override SqlCommand BuildSelectIncrementalChangesCommand(bool withFilter = false)
        {
            var sqlCommand = new SqlCommand();
            var pTimestamp = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            var pScopeId = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);

            sqlCommand.Parameters.Add(pTimestamp);
            sqlCommand.Parameters.Add(pScopeId);

            if (withFilter && this.Filters != null && this.Filters.Count > 0)
            {
                foreach (var c in this.Filters)
                {
                    if (!c.IsVirtual)
                    {
                        var columnFilter = this.tableDescription.Columns[c.ColumnName];

                        if (columnFilter == null)
                            throw new InvalidExpressionException($"Column {c.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                        var columnName = ParserName.Parse(columnFilter).Unquoted().Normalized().ToString();

                        // Get the good SqlDbType (even if we are not from Sql Server def)

                        var sqlDbType = (SqlDbType)this.sqlDbMetadata.TryGetOwnerDbType(columnFilter.OriginalDbType, columnFilter.GetDbType(), false, false, columnFilter.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                        var sqlParamFilter = new SqlParameter($"@{columnName}", sqlDbType);
                        sqlCommand.Parameters.Add(sqlParamFilter);
                    }
                    else
                    {
                        var sqlDbType = (SqlDbType)this.sqlDbMetadata.TryGetOwnerDbType(null, c.GetDbType().Value, false, false, 0, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                        var columnFilterName = ParserName.Parse(c.ColumnName).Unquoted().Normalized().ToString();
                        var sqlParamFilter = new SqlParameter($"@{columnFilterName}", sqlDbType);
                        sqlCommand.Parameters.Add(sqlParamFilter);
                    }
                }
            }

            var stringBuilder = new StringBuilder("");
            stringBuilder.AppendLine($";WITH {trackingName.Schema().Quoted().ToString()} AS (");
            stringBuilder.AppendLine($"SELECT CT.*, ");
            stringBuilder.AppendLine($"\tCASE");
            stringBuilder.AppendLine($"\t\tWHEN JSON_VALUE(CAST(CT.SYS_CHANGE_CONTEXT as char(128)), '$.sync_row_is_frozen') is null then 0");
            stringBuilder.AppendLine($"\t\tELSE 1");
            stringBuilder.AppendLine($"\tEND AS sync_row_is_frozen,");
            stringBuilder.AppendLine($"\tCAST(JSON_VALUE(CAST(CT.SYS_CHANGE_CONTEXT as char(128)), '$.update_scope_id') as uniqueidentifier) as update_scope_id,");
            stringBuilder.AppendLine($"\tCASE WHEN [CT].SYS_CHANGE_VERSION IS NULL THEN 0 ELSE [CT].SYS_CHANGE_VERSION END AS [timestamp],");
            stringBuilder.AppendLine($"\tCASE [CT].SYS_CHANGE_OPERATION WHEN 'D' THEN 1 ELSE 0 END AS sync_row_is_tombstone");
            stringBuilder.AppendLine($"From CHANGETABLE(CHANGES {tableName.Schema().Quoted().ToString()}, @sync_min_timestamp) as CT");
            stringBuilder.AppendLine($")");
            stringBuilder.AppendLine("SELECT ");
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
            stringBuilder.AppendLine($"RIGHT JOIN {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.Append($"ON ");

            string empty = "";
            foreach (var pkColumn in this.tableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.Append($"{empty}[base].{columnName} = [side].{columnName}");
                empty = " AND ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE (");
            string str = string.Empty;

            var columnFilters = this.Filters.GetColumnFilters();
            if (withFilter && columnFilters.Count() != 0)
            {
                StringBuilder builderFilter = new StringBuilder();
                builderFilter.Append("\t(");
                bool isFirst = true;
                foreach (var c in columnFilters)
                {
                    if (!isFirst)
                        builderFilter.Append(" AND ");
                    isFirst = false;

                    var columnFilter = this.tableDescription.Columns[c.ColumnName];

                    if (columnFilter == null)
                        throw new InvalidExpressionException($"Column {c.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                    var columnFilterName = ParserName.Parse(c.ColumnName).Quoted().ToString();
                    var columnFilterParameterName = ParserName.Parse(c.ColumnName).Unquoted().Normalized().ToString();

                    builderFilter.Append($"[side].{columnFilterName} = @{columnFilterParameterName}");
                }
                builderFilter.AppendLine(")");
                builderFilter.Append("\tOR (");

                isFirst = true;

                foreach (var c in columnFilters)
                {
                    if (!isFirst)
                        builderFilter.Append(" AND ");
                    isFirst = false;

                    var columnFilter = this.tableDescription.Columns[c.ColumnName];
                    var columnFilterName = ParserName.Parse(columnFilter).Quoted().ToString();

                    builderFilter.Append($"[side].{columnFilterName} IS NULL");
                }
                builderFilter.AppendLine(")");
                builderFilter.AppendLine("\t)");
                builderFilter.AppendLine("AND (");
                stringBuilder.Append(builderFilter.ToString());
            }

            stringBuilder.AppendLine("\t[side].[timestamp] > @sync_min_timestamp");
            stringBuilder.AppendLine("\t--row is not frozen AND(last updater is not the requester OR last updater is local)");
            stringBuilder.AppendLine("\tAND (([side].[sync_row_is_frozen] = 0 AND ([side].[update_scope_id] <> @sync_scope_id OR [side].[update_scope_id] IS NULL))");
            stringBuilder.AppendLine("\t-- row is frozen AND last updater is not the requester AND last updater is not local");
            stringBuilder.AppendLine("\tOR ([side].[sync_row_is_frozen] = 1 AND [side].[update_scope_id] <> @sync_scope_id AND [side].[update_scope_id] IS NOT NULL))");
            stringBuilder.AppendLine(")");

            sqlCommand.CommandText = stringBuilder.ToString();

            return sqlCommand;
        }


    }
}
