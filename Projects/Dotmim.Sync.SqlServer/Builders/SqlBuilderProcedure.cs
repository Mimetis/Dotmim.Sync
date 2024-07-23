using Dotmim.Sync.Builders;
using Dotmim.Sync.SqlServer.Manager;
using Microsoft.Data.SqlClient;
using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer.Builders
{
    /// <summary>
    /// Sql Builder for Sql Server stored procedures.
    /// </summary>
    public class SqlBuilderProcedure
    {
        private readonly SyncTable tableDescription;
        private readonly SqlObjectNames sqlObjectNames;
        private readonly SqlDbMetadata sqlDbMetadata;

        private ParserName tableName;
        private ParserName trackingName;

        /// <inheritdoc cref="SqlBuilderProcedure" />
        public SqlBuilderProcedure(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup, string scopeName)
        {
            this.tableDescription = tableDescription;
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.sqlObjectNames = new SqlObjectNames(this.tableDescription, tableName, trackingName, setup, scopeName);
            this.sqlDbMetadata = new SqlDbMetadata();
        }

        /// <summary>
        /// Get the command to check if a stored procedure exists.
        /// </summary>
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

        /// <summary>
        /// Get the command to drop a stored procedure.
        /// </summary>
        public Task<DbCommand> GetDropStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetStoredProcedureCommandName(storedProcedureType, filter);
            var text = $"DROP PROCEDURE {commandName};";

            if (storedProcedureType == DbStoredProcedureType.BulkTableType)
                text = $"DROP TYPE {commandName};";

            var sqlCommand = connection.CreateCommand();

            sqlCommand.Transaction = transaction;

            sqlCommand.CommandText = text;

            return Task.FromResult(sqlCommand);
        }

        /// <summary>
        /// Get the command to create a stored procedure.
        /// </summary>
        public Task<DbCommand> GetCreateStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            var command = storedProcedureType switch
            {
                DbStoredProcedureType.SelectChanges => this.CreateSelectIncrementalChangesCommand(connection, transaction),
                DbStoredProcedureType.SelectChangesWithFilters => this.CreateSelectIncrementalChangesWithFilterCommand(filter, connection, transaction),
                DbStoredProcedureType.SelectInitializedChanges => this.CreateSelectInitializedChangesCommand(connection, transaction),
                DbStoredProcedureType.SelectInitializedChangesWithFilters => this.CreateSelectInitializedChangesWithFilterCommand(filter, connection, transaction),
                DbStoredProcedureType.SelectRow => null,
                DbStoredProcedureType.UpdateRow => this.CreateUpdateCommand(connection, transaction),
                DbStoredProcedureType.DeleteRow => this.CreateDeleteCommand(connection, transaction),
                DbStoredProcedureType.BulkTableType => this.CreateBulkTableTypeCommand(connection, transaction),
                DbStoredProcedureType.BulkUpdateRows => this.CreateBulkUpdateCommand(connection, transaction),
                DbStoredProcedureType.BulkDeleteRows => this.CreateBulkDeleteCommand(connection, transaction),
                DbStoredProcedureType.Reset => null,
                _ => null,
            };

            return Task.FromResult(command);
        }

        /// <summary>
        /// Add the primary key columns to the command.
        /// </summary>
        protected void AddPkColumnParametersToCommand(SqlCommand sqlCommand)
        {
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
                sqlCommand.Parameters.Add(this.GetSqlParameter(pkColumn));
        }

        /// <summary>
        /// Add the columns parameters to the command.
        /// </summary>
        protected void AddColumnParametersToCommand(SqlCommand sqlCommand)
        {
            foreach (var column in this.tableDescription.Columns.Where(c => !c.IsReadOnly))
                sqlCommand.Parameters.Add(this.GetSqlParameter(column));
        }

        /// <summary>
        /// Get Sql Parameter.
        /// </summary>
        protected SqlParameter GetSqlParameter(SyncColumn column)
        {
            var sqlParameter = new SqlParameter
            {
                ParameterName = $"@{ParserName.Parse(column).Unquoted().Normalized()}",
            };

            // Get the good SqlDbType (even if we are not from Sql Server def)
            // TODO : Find something better than string comparison for change tracking provider
            var isSameProvider = this.tableDescription.OriginalProvider == SqlSyncProvider.ProviderType ||
                    this.tableDescription.OriginalProvider == "SqlSyncChangeTrackingProvider, Dotmim.Sync.SqlServer.SqlSyncChangeTrackingProvider";

            var sqlDbType = isSameProvider ?
                this.sqlDbMetadata.GetSqlDbType(column) : this.sqlDbMetadata.GetOwnerDbTypeFromDbType(column);

            sqlParameter.SqlDbType = sqlDbType;
            sqlParameter.IsNullable = column.AllowDBNull;

            var (p, s) = this.sqlDbMetadata.GetCompatibleColumnPrecisionAndScale(column, this.tableDescription.OriginalProvider);

            if (p > 0)
            {
                sqlParameter.Precision = p;
                if (s > 0)
                    sqlParameter.Scale = s;
            }

            var m = this.sqlDbMetadata.GetCompatibleMaxLength(column, this.tableDescription.OriginalProvider);

            if (m > 0)
                sqlParameter.Size = m;

            return sqlParameter;
        }

        /// <summary>
        /// From a SqlParameter, create the declaration.
        /// </summary>
        public static string CreateParameterDeclaration(SqlParameter param)
        {
            var stringBuilder3 = new StringBuilder();
            var sqlDbType = param.SqlDbType;
            var sqlDbMetadata = new SqlDbMetadata();

            if (sqlDbType == SqlDbType.Structured)
            {
                stringBuilder3.Append(string.Concat(param.ParameterName, " ", param.TypeName, " READONLY"));
            }
            else
            {
                var tmpColumn = new SyncColumn(param.ParameterName)
                {
                    OriginalDbType = sqlDbType.ToString(),
                    OriginalTypeName = sqlDbType.ToString().ToLowerInvariant(),
                    MaxLength = param.Size,
                    Precision = param.Precision,
                    Scale = param.Scale,
                    DbType = (int)param.DbType,
                };

                tmpColumn.DataType = SyncColumn.GetAssemblyQualifiedName(sqlDbMetadata.GetType(tmpColumn));

                string columnDeclarationString = sqlDbMetadata.GetCompatibleColumnTypeDeclarationString(tmpColumn, SqlSyncProvider.ProviderType);
                stringBuilder3.Append($"{param.ParameterName} {columnDeclarationString}");

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
        /// Create a stored procedure Command.
        /// </summary>
        protected DbCommand CreateProcedureCommand(Func<SqlCommand> buildCommand, string procName, DbConnection connection, DbTransaction transaction)
        {
            var cmd = buildCommand();

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

        private SqlCommand CreateProcedureCommand<T>(Func<T, SqlCommand> buildCommand, string procName, T t, DbConnection connection, DbTransaction transaction)
        {
            var cmd = buildCommand(t);

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

        /// <summary>
        /// Create bulk command.
        /// </summary>
        protected string BulkSelectUnsuccessfulRows()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("--Select all ids not inserted / deleted / updated as conflict");
            stringBuilder.Append("SELECT ");
            var pkeyComma = " ";

            foreach (var column in this.tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var cc = ParserName.Parse(column).Quoted().ToString();
                stringBuilder.Append($"{pkeyComma}[t].{cc}");
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

        /// <summary>
        /// Create a bulk delete command.
        /// </summary>
        protected virtual SqlCommand BuildBulkDeleteCommand()
        {
            var sqlCommand = new SqlCommand();

            var sqlParameter = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter);

            var sqlParameter1 = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter1);

            var sqlParameter2 = new SqlParameter("@changeTable", SqlDbType.Structured)
            {
                TypeName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkTableType),
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
                var columnType = this.sqlDbMetadata.GetCompatibleColumnTypeDeclarationString(c, this.tableDescription.OriginalProvider);
                stringBuilder.Append($"{ParserName.Parse(c).Quoted()} {columnType}, ");
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
            stringBuilder.AppendLine($"\t[side].[update_scope_id] as [sync_update_scope_id], [side].[timestamp] as [sync_timestamp], [side].[sync_row_is_tombstone] as [sync_row_is_tombstone]");
            stringBuilder.AppendLine($"\tFROM @changeTable [p]");
            stringBuilder.Append($"\tLEFT JOIN {this.trackingName.Schema().Quoted()} [side] ON ");
            stringBuilder.AppendLine($"\t{str7}");
            stringBuilder.AppendLine($"\t)");

            stringBuilder.AppendLine($"DELETE {this.tableName.Schema().Quoted()}");
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
            stringBuilder.AppendLine($"FROM {this.tableName.Quoted()} [base]");
            stringBuilder.AppendLine($"JOIN [changes] ON {str5}");
            stringBuilder.AppendLine("WHERE [changes].[sync_timestamp] <= @sync_min_timestamp OR [changes].[sync_timestamp] IS NULL OR [changes].[sync_update_scope_id] = @sync_scope_id;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- Since the delete trigger is passed, we update the tracking table to reflect the real scope deleter");
            stringBuilder.AppendLine("UPDATE [side] SET");
            stringBuilder.AppendLine("\tsync_row_is_tombstone = 1, ");
            stringBuilder.AppendLine("\tupdate_scope_id = @sync_scope_id,");
            stringBuilder.AppendLine("\tlast_change_datetime = GETUTCDATE()");
            stringBuilder.AppendLine($"FROM {this.trackingName.Schema().Quoted()} [side]");
            stringBuilder.AppendLine($"JOIN @dms_changed [t] on {str6}");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine();
            stringBuilder.Append(this.BulkSelectUnsuccessfulRows());
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        /// <summary>
        /// Create a bulk delete stored procedure command.
        /// </summary>
        public DbCommand CreateBulkDeleteCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkDeleteRows);
            return this.CreateProcedureCommand(this.BuildBulkDeleteCommand, commandName, connection, transaction);
        }

        //------------------------------------------------------------------
        // Bulk Update command
        //------------------------------------------------------------------

        /// <summary>
        /// Create bulk update command.
        /// </summary>
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
                TypeName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkTableType),
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
                var columnType = this.sqlDbMetadata.GetCompatibleColumnTypeDeclarationString(c, this.tableDescription.OriginalProvider);

                stringBuilder.Append($"{columnName} {columnType}, ");
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
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {this.tableName.Schema().Quoted()} ON;");
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
            stringBuilder.AppendLine($"\t[side].[update_scope_id] as [sync_update_scope_id], [side].[timestamp] as [sync_timestamp], [side].[sync_row_is_tombstone] as [sync_row_is_tombstone]");
            stringBuilder.AppendLine($"\tFROM @changeTable [p]");
            stringBuilder.AppendLine($"\tLEFT JOIN {this.trackingName.Schema().Quoted()} [side] ON ");
            stringBuilder.Append($"\t{str7}");
            stringBuilder.AppendLine($"\t)");

            stringBuilder.AppendLine($"MERGE {this.tableName.Schema().Quoted()} AS [base]");
            stringBuilder.AppendLine($"USING [changes] on {str5}");
            if (hasMutableColumns)
            {
                stringBuilder.AppendLine("WHEN MATCHED AND ([changes].[sync_timestamp] <= @sync_min_timestamp OR [changes].[sync_timestamp] IS NULL OR [changes].[sync_update_scope_id] = @sync_scope_id) THEN");
                foreach (var mutableColumn in this.tableDescription.Columns.Where(c => !c.IsReadOnly))
                {
                    var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                    stringBuilderArguments.Append(string.Concat(empty, columnName));
                    stringBuilderParameters.Append(string.Concat(empty, $"changes.{columnName}"));
                    empty = ", ";
                }

                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"\tUPDATE SET");

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
            stringBuilder.AppendLine($"\t({stringBuilderArguments})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters})");

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
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {this.tableName.Schema().Quoted()} ON;");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine("-- Since the update trigger is passed, we update the tracking table to reflect the real scope updater");
            stringBuilder.AppendLine("UPDATE [side] SET");
            stringBuilder.AppendLine("\t[update_scope_id] = @sync_scope_id,");
            stringBuilder.AppendLine("\t[sync_row_is_tombstone] = 0,");
            stringBuilder.AppendLine("\t[last_change_datetime] = GETUTCDATE()");
            stringBuilder.AppendLine($"FROM {this.trackingName.Schema().Quoted()} [side]");
            stringBuilder.AppendLine($"JOIN @dms_changed [t] on {str6}");

            // stringBuilder.AppendLine($"JOIN @changeTable [p] on {str7}");
            stringBuilder.AppendLine();
            stringBuilder.Append(this.BulkSelectUnsuccessfulRows());

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        /// <summary>
        /// Create bulk update stored procedure command.
        /// </summary>
        public DbCommand CreateBulkUpdateCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkUpdateRows);

            // Check if we have mutables columns
            var hasMutableColumns = this.tableDescription.GetMutableColumns(false).Any();

            return this.CreateProcedureCommand(this.BuildBulkUpdateCommand, commandName, hasMutableColumns, connection, transaction);
        }

        //------------------------------------------------------------------
        // Reset command
        //------------------------------------------------------------------

        /// <summary>
        /// Create reset command.
        /// </summary>
        protected virtual SqlCommand BuildResetCommand()
        {
            var updTriggerName = this.sqlObjectNames.GetTriggerCommandName(DbTriggerType.Update);
            var delTriggerName = this.sqlObjectNames.GetTriggerCommandName(DbTriggerType.Delete);
            var insTriggerName = this.sqlObjectNames.GetTriggerCommandName(DbTriggerType.Insert);

            SqlCommand sqlCommand = new SqlCommand();
            SqlParameter sqlParameter2 = new SqlParameter("@sync_row_count", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output,
            };
            sqlCommand.Parameters.Add(sqlParameter2);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"SET {sqlParameter2.ParameterName} = 0;");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"DISABLE TRIGGER {updTriggerName} ON {this.tableName.Schema().Quoted()};");
            stringBuilder.AppendLine($"DISABLE TRIGGER {insTriggerName} ON {this.tableName.Schema().Quoted()};");
            stringBuilder.AppendLine($"DISABLE TRIGGER {delTriggerName} ON {this.tableName.Schema().Quoted()};");

            stringBuilder.AppendLine($"DELETE FROM {this.tableName.Schema().Quoted()};");
            stringBuilder.AppendLine($"DELETE FROM {this.trackingName.Schema().Quoted()};");

            stringBuilder.AppendLine($"ENABLE TRIGGER {updTriggerName} ON {this.tableName.Schema().Quoted()};");
            stringBuilder.AppendLine($"ENABLE TRIGGER {insTriggerName} ON {this.tableName.Schema().Quoted()};");
            stringBuilder.AppendLine($"ENABLE TRIGGER {delTriggerName} ON {this.tableName.Schema().Quoted()};");

            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("SET ", sqlParameter2.ParameterName, " = @@ROWCOUNT;"));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        //------------------------------------------------------------------
        // Delete command
        //------------------------------------------------------------------

        /// <summary>
        /// Create Delete command.
        /// </summary>
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
                Direction = ParameterDirection.Output,
            };
            sqlCommand.Parameters.Add(sqlParameter2);

            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[t]", "[side]");

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
            stringBuilder.AppendLine($"DELETE {this.tableName.Schema().Quoted()}");
            stringBuilder.Append($"OUTPUT ");
            string comma = string.Empty;
            foreach (var primaryKeyColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(primaryKeyColumn).Quoted().ToString();
                stringBuilder.Append($"{comma}DELETED.{columnName}");
                comma = ", ";
            }

            stringBuilder.AppendLine($" INTO @dms_changed -- populates the temp table with successful deleted row");
            stringBuilder.AppendLine($"FROM {this.tableName.Schema().Quoted()} [base]");
            stringBuilder.Append($"LEFT JOIN {this.trackingName.Schema().Quoted()} [side] ON ");

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
            stringBuilder.AppendLine($"FROM {this.trackingName.Schema().Quoted()} [side]");
            stringBuilder.AppendLine($"JOIN @dms_changed [t] on {str6}");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine(string.Concat("SET ", sqlParameter2.ParameterName, " = @@ROWCOUNT;"));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        /// <summary>
        /// Create delete stored procedure command.
        /// </summary>
        public DbCommand CreateDeleteCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.DeleteRow);
            return this.CreateProcedureCommand(this.BuildDeleteCommand, commandName, connection, transaction);
        }

        //------------------------------------------------------------------
        // Create TVP command
        //------------------------------------------------------------------

        /// <summary>
        /// Create bulk table type command.
        /// </summary>
        private string CreateBulkTableTypeCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            var commandName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.BulkTableType);

            stringBuilder.AppendLine($"CREATE TYPE {commandName} AS TABLE (");
            string str = string.Empty;

            foreach (var c in this.tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var isPrimaryKey = this.tableDescription.IsPrimaryKey(c.ColumnName);

                var columnName = ParserName.Parse(c).Quoted().ToString();
                var nullString = isPrimaryKey ? "NOT NULL" : "NULL";

                var columnType = this.sqlDbMetadata.GetCompatibleColumnTypeDeclarationString(c, this.tableDescription.OriginalProvider);

                stringBuilder.AppendLine($"{str}{columnName} {columnType} {nullString}");
                str = ", ";
            }

            // stringBuilder.AppendLine(", [update_scope_id] [uniqueidentifier] NULL");
            stringBuilder.Append(string.Concat(str, "PRIMARY KEY ("));
            str = string.Empty;
            foreach (var c in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"{str}{columnName} ASC");
                str = ", ";
            }

            stringBuilder.Append("))");
            return stringBuilder.ToString();
        }

        /// <summary>
        /// Create bulk table type stored procedure command.
        /// </summary>
        public DbCommand CreateBulkTableTypeCommand(DbConnection connection, DbTransaction transaction)
        {
            var command = new SqlCommand(this.CreateBulkTableTypeCommandText(), (SqlConnection)connection, (SqlTransaction)transaction);
            return command;
        }

        //------------------------------------------------------------------
        // Update command
        //------------------------------------------------------------------

        /// <summary>
        /// Create update command.
        /// </summary>
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
                Direction = ParameterDirection.Output,
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
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {this.tableName.Schema().Quoted()} ON;");
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
            stringBuilder.AppendLine($"\t[side].[update_scope_id] as [sync_update_scope_id], [side].[timestamp] as [sync_timestamp], [side].[sync_row_is_tombstone] as [sync_row_is_tombstone]");
            stringBuilder.AppendLine($"\tFROM (SELECT ");
            stringBuilder.Append($"\t\t ");
            string comma = string.Empty;
            foreach (var c in this.tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                var columnParameterName = ParserName.Parse(c).Unquoted().Normalized().ToString();

                stringBuilder.Append($"{comma}@{columnParameterName} as {columnName}");
                comma = ", ";
            }

            stringBuilder.AppendLine($") AS [p]");
            stringBuilder.Append($"\tLEFT JOIN {this.trackingName.Schema().Quoted()} [side] ON ");
            stringBuilder.AppendLine($"\t{str7}");
            stringBuilder.AppendLine($"\t)");

            stringBuilder.AppendLine($"MERGE {this.tableName.Schema().Quoted()} AS [base]");
            stringBuilder.AppendLine($"USING [changes] on {str5}");
            if (hasMutableColumns)
            {
                stringBuilder.AppendLine("WHEN MATCHED AND ([changes].[sync_timestamp] <= @sync_min_timestamp OR [changes].[sync_timestamp] IS NULL OR [changes].[sync_update_scope_id] = @sync_scope_id OR @sync_force_write = 1) THEN");
                foreach (var mutableColumn in this.tableDescription.Columns.Where(c => !c.IsReadOnly))
                {
                    var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                    stringBuilderArguments.Append(string.Concat(empty, columnName));
                    stringBuilderParameters.Append(string.Concat(empty, $"changes.{columnName}"));
                    empty = ", ";
                }

                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"\tUPDATE SET");

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
            stringBuilder.AppendLine($"\t({stringBuilderArguments})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters})");

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
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {this.tableName.Schema().Quoted()} ON;");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine($"SET {sqlParameter4.ParameterName} = 0;");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine("-- Since the update trigger is passed, we update the tracking table to reflect the real scope updater");
            stringBuilder.AppendLine("UPDATE [side] SET");
            stringBuilder.AppendLine("\t[update_scope_id] = @sync_scope_id,");
            stringBuilder.AppendLine("\t[sync_row_is_tombstone] = 0,");
            stringBuilder.AppendLine("\t[last_change_datetime] = GETUTCDATE()");
            stringBuilder.AppendLine($"FROM {this.trackingName.Schema().Quoted()} [side]");
            stringBuilder.AppendLine($"JOIN @dms_changed [t] on {str6}");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"SET {sqlParameter4.ParameterName} = @@ROWCOUNT;");

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        /// <summary>
        /// Create update stored procedure command.
        /// </summary>
        public DbCommand CreateUpdateCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.UpdateRow);

            // Check if we have mutables columns
            var hasMutableColumns = this.tableDescription.GetMutableColumns(false).Any();

            return this.CreateProcedureCommand(this.BuildUpdateCommand, commandName, hasMutableColumns, (SqlConnection)connection, (SqlTransaction)transaction);
        }

        //------------------------------------------------------------------
        // Select changes command
        //------------------------------------------------------------------

        /// <summary>
        /// Add all sql parameters.
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
                    var sqlDbType = this.sqlDbMetadata.GetOwnerDbTypeFromDbType(new SyncColumn(columnName) { DbType = (int)param.DbType });

                    var customParameterFilter = new SqlParameter($"@{columnName}", sqlDbType)
                    {
                        Size = param.MaxLength,
                        IsNullable = param.AllowNull,
                        Value = param.DefaultValue,
                    };
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

                    // TODO : Find something better than string comparison for change tracking provider
                    var isSameProvider = tableFilter.OriginalProvider == SqlSyncProvider.ProviderType ||
                        tableFilter.OriginalProvider == "SqlSyncChangeTrackingProvider, Dotmim.Sync.SqlServer.SqlSyncChangeTrackingProvider";

                    var sqlDbType = isSameProvider ?
                        this.sqlDbMetadata.GetSqlDbType(columnFilter) : this.sqlDbMetadata.GetOwnerDbTypeFromDbType(columnFilter);

                    // Add it as parameter
                    var sqlParamFilter = new SqlParameter($"@{columnName}", sqlDbType)
                    {
                        Size = columnFilter.MaxLength,
                        IsNullable = param.AllowNull,
                        Value = param.DefaultValue,
                    };
                    sqlCommand.Parameters.Add(sqlParamFilter);
                }
            }
        }

        /// <summary>
        /// Create all custom joins from within a filter.
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

                var fullJoinTableName = string.IsNullOrEmpty(customJoin.TableSchemaName) ? customJoin.TableName : $"{customJoin.TableSchemaName}.{customJoin.TableName}";
                var joinTableName = ParserName.Parse(fullJoinTableName).Quoted().Schema().ToString();

                var fullLeftTableName = string.IsNullOrEmpty(customJoin.LeftTableSchemaName) ? customJoin.LeftTableName : $"{customJoin.LeftTableSchemaName}.{customJoin.LeftTableName}";
                var leftTableName = ParserName.Parse(fullLeftTableName).Quoted().Schema().ToString();

                if (string.Equals(filterTableName, leftTableName, SyncGlobalization.DataSourceStringComparison))
                    leftTableName = "[base]";

                var fullRightTableName = string.IsNullOrEmpty(customJoin.RightTableSchemaName) ? customJoin.RightTableName : $"{customJoin.RightTableSchemaName}.{customJoin.RightTableName}";
                var rightTableName = ParserName.Parse(fullRightTableName).Quoted().Schema().ToString();

                if (string.Equals(filterTableName, rightTableName, SyncGlobalization.DataSourceStringComparison))
                    rightTableName = "[base]";

                var leftColumName = ParserName.Parse(customJoin.LeftColumnName).Quoted().ToString();
                var rightColumName = ParserName.Parse(customJoin.RightColumnName).Quoted().ToString();

                stringBuilder.AppendLine($"{joinTableName} ON {leftTableName}.{leftColumName} = {rightTableName}.{rightColumName}");
            }

            return stringBuilder.ToString();
        }

        /// <summary>
        /// Create all side where criteria from within a filter.
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
        /// Create all custom wheres from witing a filter.
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
                // Template escape character
                var customWhereIteration = customWhere;
                customWhereIteration = customWhereIteration.Replace("{{{", "[");
                customWhereIteration = customWhereIteration.Replace("}}}", "]");

                stringBuilder.Append($"{and2}{customWhereIteration}");
                and2 = " AND ";
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($")");

            return stringBuilder.ToString();
        }

        /// <summary>
        /// Create select incremental changes command.
        /// </summary>
        protected virtual SqlCommand BuildSelectIncrementalChangesCommand(SyncFilter filter = null)
        {
            var sqlCommand = new SqlCommand();
            var pTimestamp = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt) { Value = 0 };
            var pScopeId = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier) { Value = "NULL", IsNullable = true }; // <--- Ok THAT's Bad, but it's working :D

            sqlCommand.Parameters.Add(pTimestamp);
            sqlCommand.Parameters.Add(pScopeId);

            // Add filter parameters
            if (filter != null)
                this.CreateFilterParameters(sqlCommand, filter);

            var stringBuilder = new StringBuilder("SELECT DISTINCT");

            // ----------------------------------
            // Add all columns
            // ----------------------------------
            foreach (var mutableColumn in this.tableDescription.GetMutableColumns(false, true))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                var isPrimaryKey = this.tableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.AppendLine($"\t[side].{columnName}, ");
                else
                    stringBuilder.AppendLine($"\t[base].{columnName}, ");
            }

            stringBuilder.AppendLine($"\t[side].[sync_row_is_tombstone] as [sync_row_is_tombstone], ");
            stringBuilder.AppendLine($"\t[side].[update_scope_id] as [sync_update_scope_id]");

            // ----------------------------------
            stringBuilder.AppendLine($"FROM {this.tableName.Schema().Quoted()} [base]");

            // ----------------------------------
            // Make Right Join
            // ----------------------------------
            stringBuilder.Append($"RIGHT JOIN {this.trackingName.Schema().Quoted()} [side] ON ");

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
                stringBuilder.Append(this.CreateFilterCustomJoins(filter));

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE (");

            // ----------------------------------
            // Where filters and Custom Where string
            // ----------------------------------
            if (filter != null)
            {
                var createFilterWhereSide = this.CreateFilterWhereSide(filter, true);
                stringBuilder.Append(createFilterWhereSide);

                if (!string.IsNullOrEmpty(createFilterWhereSide))
                    stringBuilder.AppendLine($"AND ");

                var createFilterCustomWheres = this.CreateFilterCustomWheres(filter);
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

        /// <summary>
        /// Create select incremental changes stored procedures command.
        /// </summary>
        public DbCommand CreateSelectIncrementalChangesCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectChanges);
            SqlCommand CmdWithoutFilter() => this.BuildSelectIncrementalChangesCommand(null);
            return this.CreateProcedureCommand(CmdWithoutFilter, commandName, connection, transaction);
        }

        /// <summary>
        /// Create select incremental changes stored procedures with filters command.
        /// </summary>
        public DbCommand CreateSelectIncrementalChangesWithFilterCommand(SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            if (filter == null)
                return null;

            var commandName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectChangesWithFilters, filter);
            SqlCommand CmdWithFilter() => this.BuildSelectIncrementalChangesCommand(filter);
            return this.CreateProcedureCommand(CmdWithFilter, commandName, connection, transaction);
        }

        //------------------------------------------------------------------
        // Select initialized changes command
        //------------------------------------------------------------------

        /// <summary>
        /// Create select initial changes command.
        /// </summary>
        protected virtual SqlCommand BuildSelectInitializedChangesCommand(DbConnection connection, DbTransaction transaction, SyncFilter filter = null)
        {
            var sqlCommand = new SqlCommand
            {
                CommandTimeout = 0,
            };

            var pTimestamp = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt) { Value = "NULL", IsNullable = true };
            sqlCommand.Parameters.Add(pTimestamp);

            // Add filter parameters
            if (filter != null)
                this.CreateFilterParameters(sqlCommand, filter);

            var stringBuilder = new StringBuilder();

            // if we have a filter we may have joins that will duplicate lines
            if (filter != null)
                stringBuilder.AppendLine("SELECT DISTINCT ");
            else
                stringBuilder.AppendLine("SELECT ");

            var comma = "  ";
            foreach (var mutableColumn in this.tableDescription.GetMutableColumns(false, true))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilder.AppendLine($"\t{comma}[base].{columnName}");
                comma = ", ";
            }

            stringBuilder.AppendLine($"\t, [side].[sync_row_is_tombstone] as [sync_row_is_tombstone]");
            stringBuilder.AppendLine($"FROM {this.tableName.Schema().Quoted()} [base]");

            // ----------------------------------
            // Make Left Join
            // ----------------------------------
            stringBuilder.Append($"LEFT JOIN {this.trackingName.Schema().Quoted()} [side] ON ");

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
                stringBuilder.Append(this.CreateFilterCustomJoins(filter));

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE (");

            // ----------------------------------
            // Where filters and Custom Where string
            // ----------------------------------
            if (filter != null)
            {
                var createFilterWhereSide = this.CreateFilterWhereSide(filter);
                stringBuilder.Append(createFilterWhereSide);

                if (!string.IsNullOrEmpty(createFilterWhereSide))
                    stringBuilder.AppendLine($"AND ");

                var createFilterCustomWheres = this.CreateFilterCustomWheres(filter);
                stringBuilder.Append(createFilterCustomWheres);

                if (!string.IsNullOrEmpty(createFilterCustomWheres))
                    stringBuilder.AppendLine($"AND ");
            }

            // ----------------------------------
            stringBuilder.AppendLine("\t([side].[timestamp] > @sync_min_timestamp OR  @sync_min_timestamp IS NULL)");
            stringBuilder.AppendLine(")");
            stringBuilder.AppendLine("UNION");
            stringBuilder.AppendLine("SELECT");
            comma = "  ";
            foreach (var mutableColumn in this.tableDescription.GetMutableColumns(false, true))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                var isPrimaryKey = this.tableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.AppendLine($"\t{comma}[side].{columnName}");
                else
                    stringBuilder.AppendLine($"\t{comma}[base].{columnName}");

                comma = ", ";
            }

            stringBuilder.AppendLine($"\t, [side].[sync_row_is_tombstone] as [sync_row_is_tombstone]");
            stringBuilder.AppendLine($"FROM {this.tableName.Schema().Quoted()} [base]");

            // ----------------------------------
            // Make Left Join
            // ----------------------------------
            stringBuilder.Append($"RIGHT JOIN {this.trackingName.Schema().Quoted()} [side] ON ");

            empty = string.Empty;
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.Append($"{empty}[base].{columnName} = [side].{columnName}");
                empty = " AND ";
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE ([side].[timestamp] > @sync_min_timestamp AND [side].[sync_row_is_tombstone] = 1);");

            sqlCommand.CommandText = stringBuilder.ToString();

            return sqlCommand;
        }

        /// <summary>
        /// Create select initial changes stored procedures command.
        /// </summary>
        public DbCommand CreateSelectInitializedChangesCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectInitializedChanges);
            SqlCommand CmdWithoutFilter() => this.BuildSelectInitializedChangesCommand(connection, transaction, null);
            return this.CreateProcedureCommand(CmdWithoutFilter, commandName, connection, transaction);
        }

        /// <summary>
        /// Create select initial changes stored procedures with filters command.
        /// </summary>
        public DbCommand CreateSelectInitializedChangesWithFilterCommand(SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            if (filter == null)
                return null;

            var commandName = this.sqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectInitializedChangesWithFilters, filter);
            SqlCommand CmdWithFilter() => this.BuildSelectInitializedChangesCommand(connection, transaction, filter);
            return this.CreateProcedureCommand(CmdWithFilter, commandName, connection, transaction);
        }
    }
}