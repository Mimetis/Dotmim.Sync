using Dotmim.Sync.Builders;
using Dotmim.Sync.SqlServer.Manager;
using Microsoft.Data.SqlClient;
using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlBuilderProcedure
    {
        private ParserName tableName;
        private ParserName trackingName;

        private readonly SyncTable tableDescription;
        private readonly SyncSetup setup;
        private readonly SqlObjectNames sqlObjectNames;
        private readonly SqlDbMetadata sqlDbMetadata;
        public SqlBuilderProcedure(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
        {
            this.tableDescription = tableDescription;
            this.setup = setup;
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.sqlObjectNames = new SqlObjectNames(this.tableDescription, tableName, trackingName, setup);
            this.sqlDbMetadata = new SqlDbMetadata();
        }

        public Task<DbCommand> GetExistsStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            if (filter == null && (storedProcedureType == DbStoredProcedureType.SelectChangesWithFilters || storedProcedureType == DbStoredProcedureType.SelectInitializedChangesWithFilters))
                return Task.FromResult<DbCommand>(null);

            var quotedProcedureName = this.sqlObjectNames.GetStoredProcedureCommandName(storedProcedureType, filter);

            var procedureName = ParserName.Parse(quotedProcedureName).ToString();

            var text = "IF EXISTS (SELECT * FROM sys.procedures p JOIN sys.schemas s ON s.schema_id = p.schema_id WHERE p.name = @procName AND s.name = @schemaName) SELECT 1 ELSE SELECT 0";

            if (storedProcedureType == DbStoredProcedureType.BulkTableType)
                text = "IF EXISTS (SELECT * FROM sys.types t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.name = @procName AND s.name = @schemaName) SELECT 1 ELSE SELECT 0";

            var sqlCommand = connection.CreateCommand();

            if (transaction != null)
                sqlCommand.Transaction = transaction;

            sqlCommand.CommandText = text;

            var p = sqlCommand.CreateParameter();
            p.ParameterName = "@procName";
            p.Value = procedureName;
            sqlCommand.Parameters.Add(p);

            p = sqlCommand.CreateParameter();
            p.ParameterName = "@schemaName";
            p.Value = SqlManagementUtils.GetUnquotedSqlSchemaName(ParserName.Parse(quotedProcedureName));
            sqlCommand.Parameters.Add(p);

            return Task.FromResult(sqlCommand);
        }

        public Task<DbCommand> GetDropStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)

        {
            var commandName = this.sqlObjectNames.GetStoredProcedureCommandName(storedProcedureType, filter);
            var text = $"DROP PROCEDURE {commandName};";

            if (storedProcedureType == DbStoredProcedureType.BulkTableType)
                text = $"DROP TYPE {commandName};";

            var sqlCommand = connection.CreateCommand();

            if (transaction != null)
                sqlCommand.Transaction = transaction;

            sqlCommand.CommandText = text;

            return Task.FromResult(sqlCommand);
        }

        public Task<DbCommand> GetCreateStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            var command = storedProcedureType switch
            {
                DbStoredProcedureType.SelectChanges => this.CreateSelectIncrementalChangesCommand(connection, transaction),
                DbStoredProcedureType.SelectChangesWithFilters => this.CreateSelectIncrementalChangesWithFilterCommand(filter, connection, transaction),
                DbStoredProcedureType.SelectInitializedChanges => this.CreateSelectInitializedChangesCommand(connection, transaction),
                DbStoredProcedureType.SelectInitializedChangesWithFilters => this.CreateSelectInitializedChangesWithFilterCommand(filter, connection, transaction),
                DbStoredProcedureType.SelectRow => this.CreateSelectRowCommand(connection, transaction),
                DbStoredProcedureType.UpdateRow => this.CreateUpdateCommand(connection, transaction),
                DbStoredProcedureType.DeleteRow => this.CreateDeleteCommand(connection, transaction),
                DbStoredProcedureType.DeleteMetadata => this.CreateDeleteMetadataCommand(connection, transaction),
                DbStoredProcedureType.BulkTableType => this.CreateBulkTableTypeCommand(connection, transaction),
                DbStoredProcedureType.BulkUpdateRows => this.CreateBulkUpdateCommand(connection, transaction),
                DbStoredProcedureType.BulkDeleteRows => this.CreateBulkDeleteCommand(connection, transaction),
                DbStoredProcedureType.Reset => this.CreateResetCommand(connection, transaction),
                _ => throw new NotImplementedException($"this DbStoredProcedureType {storedProcedureType} has no command to create.")
            };

            return Task.FromResult(command);
        }

        protected void AddPkColumnParametersToCommand(SqlCommand sqlCommand)
        {
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
                sqlCommand.Parameters.Add(GetSqlParameter(pkColumn));
        }
        protected void AddColumnParametersToCommand(SqlCommand sqlCommand)
        {
            foreach (var column in this.tableDescription.Columns.Where(c => !c.IsReadOnly))
                sqlCommand.Parameters.Add(GetSqlParameter(column));
        }

        protected SqlParameter GetSqlParameter(SyncColumn column)
        {
            var sqlParameter = new SqlParameter
            {
                ParameterName = $"@{ParserName.Parse(column).Unquoted().Normalized().ToString()}"
            };

            // Get the good SqlDbType (even if we are not from Sql Server def)
            var sqlDbType = (SqlDbType)this.sqlDbMetadata.TryGetOwnerDbType(column.OriginalDbType, column.GetDbType(), false, false, column.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

            sqlParameter.SqlDbType = sqlDbType;
            sqlParameter.IsNullable = column.AllowDBNull;

            var (p, s) = this.sqlDbMetadata.TryGetOwnerPrecisionAndScale(column.OriginalDbType, column.GetDbType(), false, false, column.MaxLength, column.Precision, column.Scale, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

            if (p > 0)
            {
                sqlParameter.Precision = p;
                if (s > 0)
                    sqlParameter.Scale = s;
            }

            var m = this.sqlDbMetadata.TryGetOwnerMaxLength(column.OriginalDbType, column.GetDbType(), false, false, column.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

            if (m > 0)
                sqlParameter.Size = m;

            return sqlParameter;
        }

        /// <summary>
        /// From a SqlParameter, create the declaration
        /// </summary>
        protected string CreateParameterDeclaration(SqlParameter param)
        {
            var stringBuilder3 = new StringBuilder();
            var sqlDbType = param.SqlDbType;

            string empty = this.sqlDbMetadata.GetPrecisionStringFromOwnerDbType(sqlDbType, param.Size, param.Precision, param.Scale);

            if (sqlDbType == SqlDbType.Structured)
            {
                stringBuilder3.Append(string.Concat(param.ParameterName, " ", param.TypeName, " READONLY"));
            }
            else
            {
                var sqlDbTypeString = this.sqlDbMetadata.GetStringFromOwnerDbType(sqlDbType);

                stringBuilder3.Append(string.Concat(param.ParameterName, " ", sqlDbTypeString, empty));

                if (param.Value != null)
                    stringBuilder3.Append($" = {param.Value}");
                else if (param.IsNullable)
                    stringBuilder3.Append(" = NULL");

                if (param.Direction == ParameterDirection.Output || param.Direction == ParameterDirection.InputOutput)
                    stringBuilder3.Append(" OUTPUT");
            }

            return stringBuilder3.ToString();
        }

        /// <summary>
        /// Create a stored procedure Command
        /// </summary>
        protected DbCommand CreateProcedureCommand(Func<SqlCommand> BuildCommand, string procName, DbConnection connection, DbTransaction transaction)
        {
            var cmd = BuildCommand();

            StringBuilder stringBuilder = new StringBuilder(string.Concat("CREATE PROCEDURE ", procName));
            string str = "\n\t";

            foreach (SqlParameter parameter in cmd.Parameters)
            {
                stringBuilder.Append(string.Concat(str, CreateParameterDeclaration(parameter)));
                str = ",\n\t";
            }
            stringBuilder.Append("\nAS\nBEGIN\n");
            stringBuilder.Append(cmd.CommandText);
            stringBuilder.Append("\nEND");
            var command = new SqlCommand(stringBuilder.ToString(), (SqlConnection)connection, (SqlTransaction)transaction);

            return command;
        }


        private DbCommand CreateProcedureCommand<T>(Func<T, SqlCommand> BuildCommand, string procName, T t, DbConnection connection, DbTransaction transaction)
        {
            var cmd = BuildCommand(t);

            StringBuilder stringBuilder = new StringBuilder(string.Concat("CREATE PROCEDURE ", procName));
            string str = "\n\t";

            foreach (SqlParameter parameter in cmd.Parameters)
            {
                stringBuilder.Append(string.Concat(str, CreateParameterDeclaration(parameter)));
                str = ",\n\t";
            }
            stringBuilder.Append("\nAS\nBEGIN\n");
            stringBuilder.Append(cmd.CommandText);
            stringBuilder.Append("\nEND");

            var command = new SqlCommand(stringBuilder.ToString(), (SqlConnection)connection, (SqlTransaction)transaction);

            return command;

        }


        protected string BulkSelectUnsuccessfulRows()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("--Select all ids not inserted / deleted / updated as conflict");
            stringBuilder.Append("SELECT ");
            var pkeyComma = " ";
            foreach (var primaryKeyColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var cc = ParserName.Parse(primaryKeyColumn).Quoted().ToString();
                stringBuilder.Append($"{pkeyComma}{cc}");
                pkeyComma = " ,";
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"FROM @changeTable [t]");
            stringBuilder.AppendLine("WHERE NOT EXISTS (");
            stringBuilder.Append("\t SELECT ");

            pkeyComma = " ";
            foreach (var primaryKeyColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var cc = ParserName.Parse(primaryKeyColumn).Quoted().ToString();
                stringBuilder.Append($"{pkeyComma}{cc}");
                pkeyComma = " ,";
            }

            stringBuilder.AppendLine("\t FROM @dms_changed [i]");
            stringBuilder.Append("\t WHERE ");

            pkeyComma = " ";
            foreach (var primaryKeyColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var cc = ParserName.Parse(primaryKeyColumn).Quoted().ToString();
                stringBuilder.Append($"{pkeyComma}[t].{cc} = [i].{cc}");
                pkeyComma = " AND ";
            }
            stringBuilder.AppendLine("\t)");
            return stringBuilder.ToString();
        }


        //------------------------------------------------------------------
        // Bulk Delete command
        //------------------------------------------------------------------
        protected virtual SqlCommand BuildBulkDeleteCommand()
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

            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[p]", "[t]");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[changes]", "[base]");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[t]", "[side]");
            string str7 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[p]", "[side]");

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got deleted");
            stringBuilder.Append("declare @dms_changed TABLE (");
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

            stringBuilder.AppendLine(";WITH [changes] AS (");
            stringBuilder.Append("\tSELECT ");
            foreach (var c in this.tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"[p].{columnName}, ");
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\t[side].[update_scope_id], [side].[timestamp], [side].[sync_row_is_tombstone]");
            stringBuilder.AppendLine($"\tFROM @changeTable [p]");
            stringBuilder.Append($"\tLEFT JOIN {trackingName.Schema().Quoted().ToString()} [side] ON ");
            stringBuilder.AppendLine($"\t{str7}");
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
            stringBuilder.AppendLine($"INTO @dms_changed ");
            stringBuilder.AppendLine($"FROM {tableName.Quoted().ToString()} [base]");
            stringBuilder.AppendLine($"JOIN [changes] ON {str5}");
            stringBuilder.AppendLine("WHERE [changes].[timestamp] <= @sync_min_timestamp OR [changes].[timestamp] IS NULL OR [changes].[update_scope_id] = @sync_scope_id;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- Since the delete trigger is passed, we update the tracking table to reflect the real scope deleter");
            stringBuilder.AppendLine("UPDATE [side] SET");
            stringBuilder.AppendLine("\tsync_row_is_tombstone = 1, ");
            stringBuilder.AppendLine("\tupdate_scope_id = @sync_scope_id,");
            stringBuilder.AppendLine("\tlast_change_datetime = GETUTCDATE()");
            stringBuilder.AppendLine($"FROM {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.AppendLine($"JOIN @dms_changed [t] on {str6}");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine();
            stringBuilder.Append(BulkSelectUnsuccessfulRows());
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        public DbCommand CreateBulkDeleteCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkDeleteRows);
            return CreateProcedureCommand(this.BuildBulkDeleteCommand, commandName, connection, transaction);
        }


        //------------------------------------------------------------------
        // Bulk Update command
        //------------------------------------------------------------------
        protected virtual SqlCommand BuildBulkUpdateCommand(bool hasMutableColumns)
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
                TypeName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkTableType)
            };
            sqlCommand.Parameters.Add(sqlParameter2);

            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[p]", "[t]");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[changes]", "[base]");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[t]", "[side]");
            string str7 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[p]", "[side]");

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @dms_changed TABLE (");
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

            string pkeyComma = " ";
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


            stringBuilder.AppendLine(";WITH [changes] AS (");
            stringBuilder.Append("\tSELECT ");
            foreach (var c in this.tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"[p].{columnName}, ");
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\t[side].[update_scope_id], [side].[timestamp], [side].[sync_row_is_tombstone]");
            stringBuilder.AppendLine($"\tFROM @changeTable [p]");
            stringBuilder.AppendLine($"\tLEFT JOIN {trackingName.Schema().Quoted().ToString()} [side] ON ");
            stringBuilder.Append($"\t{str7}");
            stringBuilder.AppendLine($"\t)");

            stringBuilder.AppendLine($"MERGE {tableName.Schema().Quoted().ToString()} AS [base]");
            stringBuilder.AppendLine($"USING [changes] on {str5}");
            if (hasMutableColumns)
            {
                stringBuilder.AppendLine("WHEN MATCHED AND ([changes].[timestamp] <= @sync_min_timestamp OR [changes].[timestamp] IS NULL OR [changes].[update_scope_id] = @sync_scope_id) THEN");
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


            stringBuilder.Append($"\tOUTPUT ");

            pkeyComma = " ";
            foreach (var primaryKeyColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(primaryKeyColumn).Quoted().ToString();
                stringBuilder.Append($"{pkeyComma}INSERTED.{columnName}");
                pkeyComma = ", ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\tINTO @dms_changed; -- populates the temp table with successful PKs");
            stringBuilder.AppendLine();

            // Check if we have auto inc column
            if (this.tableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {tableName.Schema().Quoted().ToString()} ON;");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine("-- Since the update trigger is passed, we update the tracking table to reflect the real scope updater");
            stringBuilder.AppendLine("UPDATE [side] SET");
            stringBuilder.AppendLine("\t[update_scope_id] = @sync_scope_id,");
            stringBuilder.AppendLine("\t[sync_row_is_tombstone] = 0,");
            stringBuilder.AppendLine("\t[last_change_datetime] = GETUTCDATE()");
            stringBuilder.AppendLine($"FROM {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.AppendLine($"JOIN @dms_changed [t] on {str6}");
            //stringBuilder.AppendLine($"JOIN @changeTable [p] on {str7}");
            stringBuilder.AppendLine();
            stringBuilder.Append(BulkSelectUnsuccessfulRows());

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        public DbCommand CreateBulkUpdateCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkUpdateRows);

            // Check if we have mutables columns
            var hasMutableColumns = this.tableDescription.GetMutableColumns(false).Any();

            return CreateProcedureCommand(BuildBulkUpdateCommand, commandName, hasMutableColumns, connection, transaction);
        }

        //------------------------------------------------------------------
        // Reset command
        //------------------------------------------------------------------
        protected virtual SqlCommand BuildResetCommand()
        {
            var updTriggerName = this.sqlObjectNames.GetTriggerCommandName(DbTriggerType.Update);
            var delTriggerName = this.sqlObjectNames.GetTriggerCommandName(DbTriggerType.Delete);
            var insTriggerName = this.sqlObjectNames.GetTriggerCommandName(DbTriggerType.Insert);

            SqlCommand sqlCommand = new SqlCommand();
            SqlParameter sqlParameter2 = new SqlParameter("@sync_row_count", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output
            };
            sqlCommand.Parameters.Add(sqlParameter2);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"SET {sqlParameter2.ParameterName} = 0;");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"DISABLE TRIGGER {updTriggerName} ON {tableName.Schema().Quoted().ToString()};");
            stringBuilder.AppendLine($"DISABLE TRIGGER {insTriggerName} ON {tableName.Schema().Quoted().ToString()};");
            stringBuilder.AppendLine($"DISABLE TRIGGER {delTriggerName} ON {tableName.Schema().Quoted().ToString()};");

            stringBuilder.AppendLine($"DELETE FROM {tableName.Schema().Quoted().ToString()};");
            stringBuilder.AppendLine($"DELETE FROM {trackingName.Schema().Quoted().ToString()};");

            stringBuilder.AppendLine($"ENABLE TRIGGER {updTriggerName} ON {tableName.Schema().Quoted().ToString()};");
            stringBuilder.AppendLine($"ENABLE TRIGGER {insTriggerName} ON {tableName.Schema().Quoted().ToString()};");
            stringBuilder.AppendLine($"ENABLE TRIGGER {delTriggerName} ON {tableName.Schema().Quoted().ToString()};");


            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("SET ", sqlParameter2.ParameterName, " = @@ROWCOUNT;"));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public DbCommand CreateResetCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.Reset);
            return CreateProcedureCommand(BuildResetCommand, commandName, connection, transaction);
        }

        //------------------------------------------------------------------
        // Delete command
        //------------------------------------------------------------------
        protected virtual SqlCommand BuildDeleteCommand()
        {
            var sqlCommand = new SqlCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);

            var sqlParameter0 = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter0);

            var sqlParameter = new SqlParameter("@sync_force_write", SqlDbType.Int);
            sqlCommand.Parameters.Add(sqlParameter);

            var sqlParameter1 = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter1);

            var sqlParameter2 = new SqlParameter("@sync_row_count", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output
            };
            sqlCommand.Parameters.Add(sqlParameter2);

            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[t]", "[side]");

            var stringBuilder = new StringBuilder();

            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @dms_changed TABLE (");
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

            var pkeyComma = " ";
            foreach (var primaryKeyColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(primaryKeyColumn).Quoted().ToString();
                stringBuilder.Append($"{pkeyComma}{columnName}");
                pkeyComma = ", ";
            }

            stringBuilder.AppendLine("));");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"DELETE {tableName.Schema().Quoted().ToString()}");
            stringBuilder.Append($"OUTPUT ");
            string comma = "";
            foreach (var primaryKeyColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(primaryKeyColumn).Quoted().ToString();
                stringBuilder.Append($"{comma}DELETED.{columnName}");
                comma = ", ";
            }
            stringBuilder.AppendLine($" INTO @dms_changed -- populates the temp table with successful deleted row");
            stringBuilder.AppendLine($"FROM {tableName.Schema().Quoted().ToString()} [base]");
            stringBuilder.Append($"JOIN {trackingName.Schema().Quoted().ToString()} [side] ON ");

            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[base]", "[side]"));

            stringBuilder.AppendLine("WHERE ([side].[timestamp] <= @sync_min_timestamp OR [side].[timestamp] IS NULL OR [side].[update_scope_id] = @sync_scope_id OR @sync_force_write = 1)");
            stringBuilder.Append("AND ");
            stringBuilder.AppendLine(string.Concat("(", SqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKeys, "[base]"), ");"));
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"SET {sqlParameter2.ParameterName} = 0;");

            stringBuilder.AppendLine("-- Since the update trigger is passed, we update the tracking table to reflect the real scope updater");
            stringBuilder.AppendLine("UPDATE [side] SET");
            stringBuilder.AppendLine("\t[update_scope_id] = @sync_scope_id,");
            stringBuilder.AppendLine("\t[sync_row_is_tombstone] = 1,");
            stringBuilder.AppendLine("\t[last_change_datetime] = GETUTCDATE()");
            stringBuilder.AppendLine($"FROM {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.AppendLine($"JOIN @dms_changed [t] on {str6}");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine(string.Concat("SET ", sqlParameter2.ParameterName, " = @@ROWCOUNT;"));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public DbCommand CreateDeleteCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.DeleteRow);
            return CreateProcedureCommand(BuildDeleteCommand, commandName, connection, transaction);
        }

        //------------------------------------------------------------------
        // Delete Metadata command
        //------------------------------------------------------------------
        protected virtual SqlCommand BuildDeleteMetadataCommand()
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
            stringBuilder.AppendLine($"DELETE [side] FROM {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.AppendLine($"WHERE [side].[timestamp] < @sync_row_timestamp");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("SET ", sqlParameter2.ParameterName, " = @@ROWCOUNT;"));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        public DbCommand CreateDeleteMetadataCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.DeleteMetadata);
            return CreateProcedureCommand(BuildDeleteMetadataCommand, commandName, (SqlConnection)connection, (SqlTransaction)transaction);
        }

        //------------------------------------------------------------------
        // Select Row command
        //------------------------------------------------------------------
        protected virtual SqlCommand BuildSelectRowCommand()
        {
            var sqlCommand = new SqlCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);
            var sqlParameter = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter);

            var stringBuilder = new StringBuilder("SELECT ");
            stringBuilder.AppendLine();
            var stringBuilder1 = new StringBuilder();
            string empty = string.Empty;
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();

                stringBuilder.AppendLine($"\t[side].{columnName}, ");
                stringBuilder1.Append($"{empty}[side].{columnName} = @{parameterName}");
                empty = " AND ";
            }
            foreach (var mutableColumn in this.tableDescription.GetMutableColumns())
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilder.AppendLine($"\t[base].{columnName}, ");
            }
            stringBuilder.AppendLine("\t[side].[sync_row_is_tombstone], ");
            stringBuilder.AppendLine("\t[side].[update_scope_id]");

            stringBuilder.AppendLine($"FROM {tableName.Schema().Quoted().ToString()} [base]");
            stringBuilder.AppendLine($"RIGHT JOIN {trackingName.Schema().Quoted().ToString()} [side] ON");

            string str = string.Empty;
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.Append($"{str}[base].{columnName} = [side].{columnName}");
                str = " AND ";
            }
            stringBuilder.AppendLine();
            stringBuilder.Append(string.Concat("WHERE ", stringBuilder1.ToString()));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        public DbCommand CreateSelectRowCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectRow);
            return CreateProcedureCommand(BuildSelectRowCommand, commandName, (SqlConnection)connection, (SqlTransaction)transaction);
        }

        //------------------------------------------------------------------
        // Create TVP command
        //------------------------------------------------------------------
        private string CreateBulkTableTypeCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            var commandName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkTableType);

            stringBuilder.AppendLine($"CREATE TYPE {commandName} AS TABLE (");
            string str = "";

            foreach (var c in this.tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var isPrimaryKey = this.tableDescription.IsPrimaryKey(c.ColumnName);

                var columnName = ParserName.Parse(c).Quoted().ToString();
                var nullString = isPrimaryKey ? "NOT NULL" : "NULL";

                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = this.sqlDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.GetDbType(), false, false, c.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var quotedColumnType = ParserName.Parse(sqlDbTypeString).Quoted().ToString();
                quotedColumnType += this.sqlDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.GetDbType(), false, false, c.MaxLength, c.Precision, c.Scale, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

                stringBuilder.AppendLine($"{str}{columnName} {quotedColumnType} {nullString}");
                str = ", ";
            }
            //stringBuilder.AppendLine(", [update_scope_id] [uniqueidentifier] NULL");
            stringBuilder.Append(string.Concat(str, "PRIMARY KEY ("));
            str = "";
            foreach (var c in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"{str}{columnName} ASC");
                str = ", ";
            }

            stringBuilder.Append("))");
            return stringBuilder.ToString();
        }
        public DbCommand CreateBulkTableTypeCommand(DbConnection connection, DbTransaction transaction)
        {
            var command = new SqlCommand(this.CreateBulkTableTypeCommandText(), (SqlConnection)connection, (SqlTransaction)transaction);
            return command;

        }


        //------------------------------------------------------------------
        // Update command
        //------------------------------------------------------------------
        protected virtual SqlCommand BuildUpdateCommand(bool hasMutableColumns)
        {
            var sqlCommand = new SqlCommand();
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();
            string empty = string.Empty;

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

            var stringBuilder = new StringBuilder();

            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[p]", "[t]");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[changes]", "[base]");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[t]", "[side]");
            string str7 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[p]", "[side]");

            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @dms_changed TABLE (");
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

            stringBuilder.AppendLine(";WITH [changes] AS (");
            stringBuilder.Append("\tSELECT ");
            foreach (var c in this.tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"[p].{columnName}, ");
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\t[side].[update_scope_id], [side].[timestamp], [side].[sync_row_is_tombstone]");
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
            stringBuilder.Append($"\tLEFT JOIN {trackingName.Schema().Quoted().ToString()} [side] ON ");
            stringBuilder.AppendLine($"\t{str7}");
            stringBuilder.AppendLine($"\t)");

            stringBuilder.AppendLine($"MERGE {tableName.Schema().Quoted().ToString()} AS [base]");
            stringBuilder.AppendLine($"USING [changes] on {str5}");
            if (hasMutableColumns)
            {
                stringBuilder.AppendLine("WHEN MATCHED AND ([changes].[timestamp] <= @sync_min_timestamp OR [changes].[timestamp] IS NULL OR [changes].[update_scope_id] = @sync_scope_id OR @sync_force_write = 1) THEN");
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

            stringBuilder.AppendLine("WHEN NOT MATCHED BY TARGET AND ([changes].[timestamp] <= @sync_min_timestamp OR [changes].[timestamp] IS NULL OR @sync_force_write = 1) THEN");


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

            pkeyComma = " ";
            foreach (var primaryKeyColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(primaryKeyColumn).Quoted().ToString();
                stringBuilder.Append($"{pkeyComma}INSERTED.{columnName}");
                pkeyComma = ", ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"INTO @dms_changed; -- populates the temp table with successful PKs");
            stringBuilder.AppendLine();

            // Check if we have auto inc column
            if (this.tableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {tableName.Schema().Quoted().ToString()} ON;");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine($"SET {sqlParameter4.ParameterName} = 0;");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine("-- Since the update trigger is passed, we update the tracking table to reflect the real scope updater");
            stringBuilder.AppendLine("UPDATE [side] SET");
            stringBuilder.AppendLine("\t[update_scope_id] = @sync_scope_id,");
            stringBuilder.AppendLine("\t[sync_row_is_tombstone] = 0,");
            stringBuilder.AppendLine("\t[last_change_datetime] = GETUTCDATE()");
            stringBuilder.AppendLine($"FROM {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.AppendLine($"JOIN @dms_changed [t] on {str6}");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"SET {sqlParameter4.ParameterName} = @@ROWCOUNT;");

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public DbCommand CreateUpdateCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.UpdateRow);

            // Check if we have mutables columns
            var hasMutableColumns = this.tableDescription.GetMutableColumns(false).Any();

            return this.CreateProcedureCommand(BuildUpdateCommand, commandName, hasMutableColumns, (SqlConnection)connection, (SqlTransaction)transaction);
        }

        /// <summary>
        /// Add all sql parameters
        /// </summary>
        protected void CreateFilterParameters(SqlCommand sqlCommand, SyncFilter filter)
        {
            var parameters = filter.Parameters;

            if (parameters.Count == 0)
                return;

            foreach (var param in parameters)
            {
                if (param.DbType.HasValue)
                {
                    // Get column name and type
                    var columnName = ParserName.Parse(param.Name).Unquoted().Normalized().ToString();
                    var sqlDbType = (SqlDbType)this.sqlDbMetadata.TryGetOwnerDbType(null, param.DbType.Value, false, false, param.MaxLength, SqlSyncProvider.ProviderType, SqlSyncProvider.ProviderType);

                    var customParameterFilter = new SqlParameter($"@{columnName}", sqlDbType);
                    customParameterFilter.Size = param.MaxLength;
                    customParameterFilter.IsNullable = param.AllowNull;
                    customParameterFilter.Value = param.DefaultValue;
                    sqlCommand.Parameters.Add(customParameterFilter);
                }
                else
                {
                    var tableFilter = this.tableDescription.Schema.Tables[param.TableName, param.SchemaName];
                    if (tableFilter == null)
                        throw new FilterParamTableNotExistsException(param.TableName);

                    var columnFilter = tableFilter.Columns[param.Name];
                    if (columnFilter == null)
                        throw new FilterParamColumnNotExistsException(param.Name, param.TableName);

                    // Get column name and type
                    var columnName = ParserName.Parse(columnFilter).Unquoted().Normalized().ToString();
                    var sqlDbType = (SqlDbType)this.sqlDbMetadata.TryGetOwnerDbType(columnFilter.OriginalDbType, columnFilter.GetDbType(), false, false, columnFilter.MaxLength, tableFilter.OriginalProvider, SqlSyncProvider.ProviderType);

                    // Add it as parameter
                    var sqlParamFilter = new SqlParameter($"@{columnName}", sqlDbType);
                    sqlParamFilter.Size = columnFilter.MaxLength;
                    sqlParamFilter.IsNullable = param.AllowNull;
                    sqlParamFilter.Value = param.DefaultValue;
                    sqlCommand.Parameters.Add(sqlParamFilter);
                }

            }
        }

        /// <summary>
        /// Create all custom joins from within a filter 
        /// </summary>
        protected string CreateFilterCustomJoins(SyncFilter filter)
        {
            var customJoins = filter.Joins;

            if (customJoins.Count == 0)
                return string.Empty;

            var stringBuilder = new StringBuilder();

            stringBuilder.AppendLine();
            foreach (var customJoin in customJoins)
            {
                switch (customJoin.JoinEnum)
                {
                    case Join.Left:
                        stringBuilder.Append("LEFT JOIN ");
                        break;
                    case Join.Right:
                        stringBuilder.Append("RIGHT JOIN ");
                        break;
                    case Join.Outer:
                        stringBuilder.Append("OUTER JOIN ");
                        break;
                    case Join.Inner:
                    default:
                        stringBuilder.Append("INNER JOIN ");
                        break;
                }

                var fullTableName = string.IsNullOrEmpty(filter.SchemaName) ? filter.TableName : $"{filter.SchemaName}.{filter.TableName}";
                var filterTableName = ParserName.Parse(fullTableName).Quoted().Schema().ToString();

                var joinTableName = ParserName.Parse(customJoin.TableName).Quoted().Schema().ToString();

                var leftTableName = ParserName.Parse(customJoin.LeftTableName).Quoted().Schema().ToString();
                if (string.Equals(filterTableName, leftTableName, SyncGlobalization.DataSourceStringComparison))
                    leftTableName = "[base]";

                var rightTableName = ParserName.Parse(customJoin.RightTableName).Quoted().Schema().ToString();
                if (string.Equals(filterTableName, rightTableName, SyncGlobalization.DataSourceStringComparison))
                    rightTableName = "[base]";

                var leftColumName = ParserName.Parse(customJoin.LeftColumnName).Quoted().ToString();
                var rightColumName = ParserName.Parse(customJoin.RightColumnName).Quoted().ToString();

                stringBuilder.AppendLine($"{joinTableName} ON {leftTableName}.{leftColumName} = {rightTableName}.{rightColumName}");
            }

            return stringBuilder.ToString();
        }

        /// <summary>
        /// Create all side where criteria from within a filter
        /// </summary>
        protected string CreateFilterWhereSide(SyncFilter filter, bool checkTombstoneRows = false)
        {
            var sideWhereFilters = filter.Wheres;

            if (sideWhereFilters.Count == 0)
                return string.Empty;

            var stringBuilder = new StringBuilder();
            // Managing when state is tombstone
            if (checkTombstoneRows)
                stringBuilder.AppendLine($"(");

            stringBuilder.AppendLine($" (");


            var and2 = "   ";

            foreach (var whereFilter in sideWhereFilters)
            {
                var tableFilter = this.tableDescription.Schema.Tables[whereFilter.TableName, whereFilter.SchemaName];
                if (tableFilter == null)
                    throw new FilterParamTableNotExistsException(whereFilter.TableName);

                var columnFilter = tableFilter.Columns[whereFilter.ColumnName];
                if (columnFilter == null)
                    throw new FilterParamColumnNotExistsException(whereFilter.ColumnName, whereFilter.TableName);

                var tableName = ParserName.Parse(tableFilter).Unquoted().ToString();
                if (string.Equals(tableName, filter.TableName, SyncGlobalization.DataSourceStringComparison))
                    tableName = "[base]";
                else
                    tableName = ParserName.Parse(tableFilter).Quoted().Schema().ToString();

                var columnName = ParserName.Parse(columnFilter).Quoted().ToString();
                var parameterName = ParserName.Parse(whereFilter.ParameterName).Unquoted().Normalized().ToString();
                var sqlDbType = (SqlDbType)this.sqlDbMetadata.TryGetOwnerDbType(columnFilter.OriginalDbType, columnFilter.GetDbType(), false, false, columnFilter.MaxLength, tableFilter.OriginalProvider, SqlSyncProvider.ProviderType);

                var param = filter.Parameters[parameterName];

                if (param == null)
                    throw new FilterParamColumnNotExistsException(columnName, whereFilter.TableName);

                stringBuilder.Append($"{and2}({tableName}.{columnName} = @{parameterName}");

                if (param.AllowNull)
                    stringBuilder.Append($" OR @{parameterName} IS NULL");

                stringBuilder.Append($")");

                and2 = " AND ";

            }
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"  )");

            if (checkTombstoneRows)
            {
                stringBuilder.AppendLine($" OR [side].[sync_row_is_tombstone] = 1");
                stringBuilder.AppendLine($")");
            }
            // Managing when state is tombstone


            return stringBuilder.ToString();
        }

        /// <summary>
        /// Create all custom wheres from witing a filter
        /// </summary>
        protected string CreateFilterCustomWheres(SyncFilter filter)
        {
            var customWheres = filter.CustomWheres;

            if (customWheres.Count == 0)
                return string.Empty;

            var stringBuilder = new StringBuilder();
            var and2 = "  ";
            stringBuilder.AppendLine($"(");

            foreach (var customWhere in customWheres)
            {
                stringBuilder.Append($"{and2}{customWhere}");
                and2 = " AND ";
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($")");

            return stringBuilder.ToString();
        }


        //------------------------------------------------------------------
        // Select changes command
        //------------------------------------------------------------------
        protected virtual SqlCommand BuildSelectIncrementalChangesCommand(SyncFilter filter = null)
        {
            var sqlCommand = new SqlCommand();
            var pTimestamp = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt) { Value = 0 };
            var pScopeId = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier) { Value = "NULL", IsNullable = true }; // <--- Ok THAT's Bad, but it's working :D

            sqlCommand.Parameters.Add(pTimestamp);
            sqlCommand.Parameters.Add(pScopeId);

            // Add filter parameters
            if (filter != null)
                CreateFilterParameters(sqlCommand, filter);

            var stringBuilder = new StringBuilder("SELECT DISTINCT");

            // ----------------------------------
            // Add all columns
            // ----------------------------------
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
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
            // ----------------------------------

            stringBuilder.AppendLine($"FROM {tableName.Schema().Quoted().ToString()} [base]");

            // ----------------------------------
            // Make Right Join
            // ----------------------------------
            stringBuilder.Append($"RIGHT JOIN {trackingName.Schema().Quoted().ToString()} [side] ON ");

            string empty = "";
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
            // Where filters and Custom Where string
            // ----------------------------------
            if (filter != null)
            {
                var createFilterWhereSide = CreateFilterWhereSide(filter, true);
                stringBuilder.Append(createFilterWhereSide);

                if (!string.IsNullOrEmpty(createFilterWhereSide))
                    stringBuilder.AppendLine($"AND ");

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
        public DbCommand CreateSelectIncrementalChangesCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectChanges);
            SqlCommand cmdWithoutFilter() => BuildSelectIncrementalChangesCommand(null);
            return this.CreateProcedureCommand(cmdWithoutFilter, commandName, connection, transaction);
        }
        public DbCommand CreateSelectIncrementalChangesWithFilterCommand(SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            if (filter == null)
                return null;

            var commandName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectChangesWithFilters, filter);
            SqlCommand cmdWithFilter() => BuildSelectIncrementalChangesCommand(filter);
            return this.CreateProcedureCommand(cmdWithFilter, commandName, connection, transaction);
        }

        //------------------------------------------------------------------
        // Select initialized changes command
        //------------------------------------------------------------------
        protected virtual SqlCommand BuildSelectInitializedChangesCommand(DbConnection connection, DbTransaction transaction, SyncFilter filter = null)
        {

            var sqlCommand = new SqlCommand();
            // Add filter parameters
            if (filter != null)
                this.CreateFilterParameters(sqlCommand, filter);

            var stringBuilder = new StringBuilder("SELECT DISTINCT");
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

            if (filter != null)
            {
                // ----------------------------------
                // Custom Joins
                // ----------------------------------
                stringBuilder.Append(CreateFilterCustomJoins(filter));

                // ----------------------------------
                // Where filters on [side]
                // ----------------------------------

                var whereString = CreateFilterWhereSide(filter);
                var customWhereString = CreateFilterCustomWheres(filter);

                if (!string.IsNullOrEmpty(whereString) || !string.IsNullOrEmpty(customWhereString))
                {
                    stringBuilder.AppendLine("WHERE");

                    if (!string.IsNullOrEmpty(whereString))
                        stringBuilder.AppendLine(whereString);

                    if (!string.IsNullOrEmpty(whereString) && !string.IsNullOrEmpty(customWhereString))
                        stringBuilder.AppendLine("AND");

                    if (!string.IsNullOrEmpty(customWhereString))
                        stringBuilder.AppendLine(customWhereString);
                }
            }
            // ----------------------------------


            sqlCommand.CommandText = stringBuilder.ToString();

            return sqlCommand;
        }

        public DbCommand CreateSelectInitializedChangesCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectInitializedChanges);
            SqlCommand cmdWithoutFilter() => BuildSelectInitializedChangesCommand(connection, transaction, null);
            return this.CreateProcedureCommand(cmdWithoutFilter, commandName, connection, transaction);
        }

        public DbCommand CreateSelectInitializedChangesWithFilterCommand(SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            if (filter == null)
                return null;

            var commandName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectInitializedChangesWithFilters, filter);
            SqlCommand cmdWithFilter() => BuildSelectInitializedChangesCommand(connection, transaction, filter);
            return this.CreateProcedureCommand(cmdWithFilter, commandName, connection, transaction);

        }
    }
}