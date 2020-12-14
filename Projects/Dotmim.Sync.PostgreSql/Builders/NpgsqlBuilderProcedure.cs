using Dotmim.Sync.Builders;
using Dotmim.Sync.Postgres.Scope;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Postgres.Builders
{
    public class NpgsqlBuilderProcedure : IDbBuilderStoreProcedureCommands
    {
        private ParserName tableName;
        private ParserName trackingName;
        private readonly SyncTable tableDescription;
        private readonly SyncSetup setup;
        private readonly NpgsqlObjectNames sqlObjectNames;
        private readonly NpgsqlDbMetadata sqlDbMetadata;
        internal const string NPGSQL_PREFIX_PARAMETER = "in_";

        public NpgsqlBuilderProcedure(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
        {
            this.tableDescription = tableDescription;
            this.setup = setup;
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.sqlObjectNames = new NpgsqlObjectNames(this.tableDescription, setup);
            this.sqlDbMetadata = new NpgsqlDbMetadata();
        }

        protected void AddPkColumnParametersToCommand(NpgsqlCommand sqlCommand)
        {
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
                sqlCommand.Parameters.Add(GetNpgsqlParameter(pkColumn));
        }
        protected void AddColumnParametersToCommand(NpgsqlCommand sqlCommand)
        {
            foreach (var column in this.tableDescription.Columns.Where(c => !c.IsReadOnly))
                sqlCommand.Parameters.Add(GetNpgsqlParameter(column));
        }

        protected NpgsqlParameter GetNpgsqlParameter(SyncColumn column)
        {
            var parameterName = ParserName.Parse(column).Unquoted().Normalized().ToString();
            var sqlParameter = new NpgsqlParameter
            {
                ParameterName = $"\"{NpgsqlBuilderProcedure.NPGSQL_PREFIX_PARAMETER}{parameterName}\"",
            };

            // Get the good NpgsqlDbType (even if we are not from Sql Server def)
            var npgsqlDbType = (NpgsqlDbType)this.sqlDbMetadata.TryGetOwnerDbType(column.OriginalDbType, column.GetDbType(), false, false, column.MaxLength, this.tableDescription.OriginalProvider, NpgsqlSyncProvider.ProviderType);

            sqlParameter.NpgsqlDbType = npgsqlDbType;
            sqlParameter.IsNullable = column.AllowDBNull;

            var (p, s) = this.sqlDbMetadata.TryGetOwnerPrecisionAndScale(column.OriginalDbType, column.GetDbType(), false, false, column.MaxLength, column.Precision, column.Scale, this.tableDescription.OriginalProvider, NpgsqlSyncProvider.ProviderType);

            if (p > 0)
            {
                sqlParameter.Precision = p;
                if (s > 0)
                    sqlParameter.Scale = s;
            }

            var m = this.sqlDbMetadata.TryGetOwnerMaxLength(column.OriginalDbType, column.GetDbType(), false, false, column.MaxLength, this.tableDescription.OriginalProvider, NpgsqlSyncProvider.ProviderType);

            if (m > 0)
                sqlParameter.Size = m;

            return sqlParameter;
        }

        /// <summary>
        /// From a NpgsqlParameter, create the declaration
        /// </summary>
        protected string CreateParameterDeclaration(NpgsqlParameter param)
        {
            var stringBuilder3 = new StringBuilder();
            var sqlDbType = param.NpgsqlDbType;

            string empty = this.sqlDbMetadata.GetPrecisionStringFromOwnerDbType(sqlDbType, param.Size, param.Precision, param.Scale);


            var sqlDbTypeString = this.sqlDbMetadata.GetStringFromOwnerDbType(sqlDbType);

            stringBuilder3.Append(string.Concat(param.ParameterName, " ", sqlDbTypeString, empty));

            if (param.Value != null)
                stringBuilder3.Append($" = {param.Value}");
            else if (param.IsNullable)
                stringBuilder3.Append(" = NULL");

            //if (param.Direction == ParameterDirection.Output || param.Direction == ParameterDirection.InputOutput)
            //    stringBuilder3.Append(" OUTPUT");


            return stringBuilder3.ToString();
        }

        /// <summary>
        /// From a NpgsqlCommand, create a stored procedure string
        /// </summary>
        private string CreateProcedureCommandText(NpgsqlCommand cmd, string procName)
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE OR REPLACE FUNCTION {procName}(");
            string str = "\n\t";
            foreach (NpgsqlParameter parameter in cmd.Parameters)
            {
                stringBuilder.Append(string.Concat(str, CreateParameterDeclaration(parameter)));
                str = ",\n\t";
            }
            stringBuilder.AppendLine("\n\t)");
            stringBuilder.AppendLine("\n\tRETURNS void");
            stringBuilder.AppendLine("\n\tLANGUAGE 'plpgsql'");
            stringBuilder.AppendLine("AS $BODY$");
            stringBuilder.AppendLine("BEGIN");
            stringBuilder.AppendLine(cmd.CommandText);
            stringBuilder.AppendLine("END;");
            stringBuilder.AppendLine("$BODY$;");
            return stringBuilder.ToString();
        }

        /// <summary>
        /// Create a stored procedure
        /// </summary>
        protected async Task CreateProcedureCommandAsync(Func<NpgsqlCommand> BuildCommand, string procName, DbConnection connection, DbTransaction transaction)
        {
            var str = CreateProcedureCommandText(BuildCommand(), procName);

            using (var command = new NpgsqlCommand(str, (NpgsqlConnection)connection, (NpgsqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }


        private async Task CreateProcedureCommandAsync<T>(Func<T, NpgsqlCommand> BuildCommand, string procName, T t, DbConnection connection, DbTransaction transaction)
        {

            var str = CreateProcedureCommandText(BuildCommand(t), procName);

            using (var command = new NpgsqlCommand(str, (NpgsqlConnection)connection, (NpgsqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }

        }


        /// <summary>
        /// Check if we need to create the stored procedure
        /// </summary>
        public async Task<bool> NeedToCreateProcedureAsync(DbCommandType commandType, DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetCommandName(commandType).name;

            return !await NpgsqlManagementUtils.ProcedureExistsAsync((NpgsqlConnection)connection, (NpgsqlTransaction)transaction, commandName).ConfigureAwait(false);
        }

        /// <summary>
        /// Check if we need to create the TVP Type
        /// </summary>
        public async Task<bool> NeedToCreateTypeAsync(DbCommandType commandType, DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetCommandName(commandType).name;

            return !await NpgsqlManagementUtils.TypeExistsAsync((NpgsqlConnection)connection, (NpgsqlTransaction)transaction, commandName).ConfigureAwait(false);
        }

        protected string BulkSelectUnsuccessfulRows()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("--Select all ids not inserted / deleted / updated as conflict");
            stringBuilder.Append("SELECT ");
            for (int i = 0; i < this.tableDescription.PrimaryKeys.Count; i++)
            {
                var cc = ParserName.Parse(this.tableDescription.PrimaryKeys[i]).Quoted().ToString();

                stringBuilder.Append($"{cc}");
                if (i < this.tableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine($"FROM @changeTable [t]");
            stringBuilder.AppendLine("WHERE NOT EXISTS (");
            stringBuilder.Append("\t SELECT ");
            for (int i = 0; i < this.tableDescription.PrimaryKeys.Count; i++)
            {
                var cc = ParserName.Parse(this.tableDescription.PrimaryKeys[i])
                                   .Quoted().ToString();

                stringBuilder.Append($"{cc}");
                if (i < this.tableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine("\t FROM @dms_changed [i]");
            stringBuilder.Append("\t WHERE ");
            for (int i = 0; i < this.tableDescription.PrimaryKeys.Count; i++)
            {
                var cc = ParserName.Parse(this.tableDescription.PrimaryKeys[i])
                                .Quoted().ToString();
                stringBuilder.Append($"[t].{cc} = [i].{cc}");
                if (i < this.tableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append("AND ");
                else
                    stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine("\t)");
            return stringBuilder.ToString();
        }


        //------------------------------------------------------------------
        // Reset command
        //------------------------------------------------------------------
        protected virtual NpgsqlCommand BuildResetCommand()
        {
            var updTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger).name;
            var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger).name;
            var insTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger).name;

            NpgsqlCommand sqlCommand = new NpgsqlCommand();
            NpgsqlParameter sqlParameter2 = new NpgsqlParameter("@sync_row_count", NpgsqlDbType.Integer)
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
        public Task CreateResetAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.Reset).name;
            return CreateProcedureCommandAsync(BuildResetCommand, commandName, connection, transaction);
        }
        public async Task DropResetAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.Reset).name;
            var commandText = $"DROP FUNCTION \"{commandName};\"";

            using (var command = new NpgsqlCommand(commandText, (NpgsqlConnection)connection, (NpgsqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        //------------------------------------------------------------------
        // Delete command
        //------------------------------------------------------------------
        protected virtual NpgsqlCommand BuildDeleteCommand()
        {
            var sqlCommand = new NpgsqlCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);

            var sqlParameter0 = new NpgsqlParameter("sync_scope_id", NpgsqlDbType.Uuid);
            sqlCommand.Parameters.Add(sqlParameter0);

            var sqlParameter = new NpgsqlParameter("sync_force_write", NpgsqlDbType.Boolean);
            sqlCommand.Parameters.Add(sqlParameter);

            var sqlParameter1 = new NpgsqlParameter("sync_min_timestamp", NpgsqlDbType.Bigint);
            sqlCommand.Parameters.Add(sqlParameter1);

            var sqlParameter2 = new NpgsqlParameter("sync_row_count", NpgsqlDbType.Integer)
            {
                Direction = ParameterDirection.Output
            };
            sqlCommand.Parameters.Add(sqlParameter2);

            string str6 = NpgsqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[t]", "[side]");

            var stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"WITH \"DEL\" AS (");
            stringBuilder.AppendLine($" DELETE FROM {tableName.Schema().Quoted().ToString()} as base");
            stringBuilder.AppendLine($" WHERE ({NpgsqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKeys, "base")})");
            stringBuilder.AppendLine($" AND EXISTS (");
            stringBuilder.AppendLine($"  SELECT * FROM {trackingName.Schema().Quoted().ToString()} as side");
            stringBuilder.AppendLine($"  WHERE ({NpgsqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "base", "side")})");
            stringBuilder.AppendLine($"  AND (side.timestamp <= {NpgsqlBuilderProcedure.NPGSQL_PREFIX_PARAMETER}sync_min_timestamp");
            stringBuilder.AppendLine($"       OR side.timestamp IS NULL");
            stringBuilder.AppendLine($"       OR side.update_scope_id = {NpgsqlBuilderProcedure.NPGSQL_PREFIX_PARAMETER}sync_scope_id");
            stringBuilder.AppendLine($"       OR {NpgsqlBuilderProcedure.NPGSQL_PREFIX_PARAMETER}sync_force_write = true)");
            stringBuilder.AppendLine($"  )");
            stringBuilder.AppendLine($" RETURNING *");
            stringBuilder.AppendLine($")");
            stringBuilder.AppendLine($"UPDATE {trackingName.Schema().Quoted().ToString()} SET");
            stringBuilder.AppendLine($"\tupdate_scope_id = {NpgsqlBuilderProcedure.NPGSQL_PREFIX_PARAMETER}sync_scope_id,");
            stringBuilder.AppendLine($"\tsync_row_is_tombstone = true,");
            stringBuilder.AppendLine($"\ttimestamp = {NpgsqlScopeInfoBuilder.TimestampValue},");
            stringBuilder.AppendLine($"\tlast_change_datetime = now()");
            stringBuilder.AppendLine($"FROM \"DEL\"");
            stringBuilder.AppendLine($"WHERE ({NpgsqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, trackingName.Schema().Quoted().ToString(), "\"DEL\"")});");
            stringBuilder.AppendLine();

            sqlCommand.CommandText = stringBuilder.ToString();

            return sqlCommand;
        }
        public Task CreateDeleteAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteRow).name;
            return CreateProcedureCommandAsync(BuildDeleteCommand, commandName, connection, transaction);
        }
        public async Task DropDeleteAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteRow).name;
            var commandText = $"DROP PROCEDURE {commandName};";

            using (var command = new NpgsqlCommand(commandText, (NpgsqlConnection)connection, (NpgsqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        //------------------------------------------------------------------
        // Delete Metadata command
        //------------------------------------------------------------------
        protected virtual NpgsqlCommand BuildDeleteMetadataCommand()
        {
            NpgsqlCommand sqlCommand = new NpgsqlCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);
            NpgsqlParameter sqlParameter1 = new NpgsqlParameter("sync_row_timestamp", NpgsqlDbType.Bigint);
            sqlCommand.Parameters.Add(sqlParameter1);
            NpgsqlParameter sqlParameter2 = new NpgsqlParameter("@sync_row_count", NpgsqlDbType.Integer)
            {
                Direction = ParameterDirection.Output
            };
            sqlCommand.Parameters.Add(sqlParameter2);
            StringBuilder stringBuilder = new StringBuilder();
            //stringBuilder.AppendLine($"SET {sqlParameter2.ParameterName} = 0;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"DELETE FROM {trackingName.Schema().Quoted().ToString()} ");
            stringBuilder.AppendLine($"WHERE timestamp < {NpgsqlBuilderProcedure.NPGSQL_PREFIX_PARAMETER}sync_row_timestamp");
            stringBuilder.AppendLine();
            //stringBuilder.AppendLine(string.Concat("SET ", sqlParameter2.ParameterName, " = @@ROWCOUNT;"));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public Task CreateDeleteMetadataAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteMetadata).name;
            return CreateProcedureCommandAsync(BuildDeleteMetadataCommand, commandName, connection, transaction);
        }
        public async Task DropDeleteMetadataAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteMetadata).name;
            var commandText = $"DROP PROCEDURE {commandName};";

            using (var command = new NpgsqlCommand(commandText, (NpgsqlConnection)connection, (NpgsqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        //------------------------------------------------------------------
        // Select Row command
        //------------------------------------------------------------------
        protected virtual NpgsqlCommand BuildSelectRowCommand()
        {
            var sqlCommand = new NpgsqlCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);
            var sqlParameter = new NpgsqlParameter($"{NpgsqlBuilderProcedure.NPGSQL_PREFIX_PARAMETER}sync_scope_id", NpgsqlDbType.Uuid);
            sqlCommand.Parameters.Add(sqlParameter);

            var stringBuilder = new StringBuilder("SELECT ");
            stringBuilder.AppendLine();
            var stringBuilder1 = new StringBuilder();
            string empty = string.Empty;
            foreach (var pkColumn in this.tableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn, "\"").Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn, "\"").Unquoted().Normalized().ToString();

                stringBuilder.AppendLine($"\tside.{columnName}, ");
                stringBuilder1.Append($"{empty}side.{columnName} = \"{NpgsqlBuilderProcedure.NPGSQL_PREFIX_PARAMETER}{parameterName}\"");
                empty = " AND ";
            }
            foreach (var mutableColumn in this.tableDescription.GetMutableColumns())
            {
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                stringBuilder.AppendLine($"\tbase.{columnName}, ");
            }
            stringBuilder.AppendLine("\tside.sync_row_is_tombstone, ");
            stringBuilder.AppendLine("\tside.update_scope_id");

            stringBuilder.AppendLine($"FROM {tableName.Schema().Quoted().ToString()} base");
            stringBuilder.AppendLine($"RIGHT JOIN {trackingName.Schema().Quoted().ToString()} side ON");

            string str = string.Empty;
            foreach (var pkColumn in this.tableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn, "\"").Quoted().ToString();
                stringBuilder.Append($"{str}base.{columnName} = side.{columnName}");
                str = " AND ";
            }
            stringBuilder.AppendLine();
            stringBuilder.Append($"WHERE {stringBuilder1.ToString()};");
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public Task CreateSelectRowAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectRow).name;
            return CreateProcedureCommandAsync(BuildSelectRowCommand, commandName, connection, transaction);
        }


        public async Task DropSelectRowAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectRow).name;
            var commandText = $"DROP PROCEDURE {commandName};";

            using (var command = new NpgsqlCommand(commandText, (NpgsqlConnection)connection, (NpgsqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        //------------------------------------------------------------------
        // Update command
        //------------------------------------------------------------------
        protected virtual NpgsqlCommand BuildUpdateCommand(bool hasMutableColumns)
        {

            var sqlCommand = new NpgsqlCommand();
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();
            string empty = string.Empty;

            this.AddColumnParametersToCommand(sqlCommand);

            var sqlParameter1 = new NpgsqlParameter("sync_min_timestamp", NpgsqlDbType.Bigint);
            sqlCommand.Parameters.Add(sqlParameter1);

            var sqlParameter2 = new NpgsqlParameter("sync_scope_id", NpgsqlDbType.Uuid);
            sqlCommand.Parameters.Add(sqlParameter2);

            var sqlParameter3 = new NpgsqlParameter("sync_force_write", NpgsqlDbType.Boolean);
            sqlCommand.Parameters.Add(sqlParameter3);

            var sqlParameter4 = new NpgsqlParameter("sync_row_count", NpgsqlDbType.Integer)
            {
                Direction = ParameterDirection.Output
            };
            sqlCommand.Parameters.Add(sqlParameter4);

            var stringBuilder = new StringBuilder();

            string str4 = NpgsqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[p]", "[t]");
            string str5 = NpgsqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[changes]", "[base]");
            string str6 = NpgsqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[t]", "[side]");
            string str7 = NpgsqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[p]", "[side]");

            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @dms_changed TABLE (");
            foreach (var c in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(c, "\"").Quoted().ToString();

                // Get the good NpgsqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = this.sqlDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.GetDbType(), false, false, c.MaxLength, this.tableDescription.OriginalProvider, NpgsqlSyncProvider.ProviderType);
                var quotedColumnType = ParserName.Parse(sqlDbTypeString, "\"").Quoted().ToString();
                quotedColumnType += this.sqlDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.GetDbType(), false, false, c.MaxLength, c.Precision, c.Scale, this.tableDescription.OriginalProvider, NpgsqlSyncProvider.ProviderType);

                stringBuilder.Append($"{columnName} {quotedColumnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < this.tableDescription.PrimaryKeys.Count; i++)
            {
                var columnName = ParserName.Parse(this.tableDescription.PrimaryKeys[i], "\"").Quoted().ToString();
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

            stringBuilder.AppendLine(";WITH [changes] AS (");
            stringBuilder.Append("\tSELECT ");
            foreach (var c in this.tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnName = ParserName.Parse(c, "\"").Quoted().ToString();
                stringBuilder.Append($"[p].{columnName}, ");
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\t[side].[update_scope_id], [side].[timestamp], [side].[sync_row_is_tombstone]");
            stringBuilder.AppendLine($"\tFROM (SELECT ");
            stringBuilder.Append($"\t\t ");
            string comma = "";
            foreach (var c in this.tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnName = ParserName.Parse(c, "\"").Quoted().ToString();
                var columnParameterName = ParserName.Parse(c, "\"").Unquoted().Normalized().ToString();

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
                    var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                    stringBuilderArguments.Append(string.Concat(empty, columnName));
                    stringBuilderParameters.Append(string.Concat(empty, $"changes.{columnName}"));
                    empty = ", ";
                }
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"\tUPDATE SET");

                string strSeparator = "";
                foreach (var mutableColumn in this.tableDescription.GetMutableColumns(false))
                {
                    var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
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
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();

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
                var columnName = ParserName.Parse(this.tableDescription.PrimaryKeys[i], "\"").Quoted().ToString();
                stringBuilder.Append($"INSERTED.{columnName}");
                if (i < this.tableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }
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
        public Task CreateUpdateAsync(bool hasMutableColumns, DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateRow).name;
            return this.CreateProcedureCommandAsync(BuildUpdateCommand, commandName, hasMutableColumns, connection, transaction);
        }
        public async Task DropUpdateAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateRow).name;
            var commandText = $"DROP PROCEDURE {commandName};";

            using (var command = new NpgsqlCommand(commandText, (NpgsqlConnection)connection, (NpgsqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Add all sql parameters
        /// </summary>
        protected void CreateFilterParameters(NpgsqlCommand sqlCommand, SyncFilter filter)
        {
            var parameters = filter.Parameters;

            if (parameters.Count == 0)
                return;

            foreach (var param in parameters)
            {
                if (param.DbType.HasValue)
                {
                    // Get column name and type
                    var columnName = ParserName.Parse(param.Name, "\"").Unquoted().Normalized().ToString();
                    var sqlDbType = (NpgsqlDbType)this.sqlDbMetadata.TryGetOwnerDbType(null, param.DbType.Value, false, false, param.MaxLength, NpgsqlSyncProvider.ProviderType, NpgsqlSyncProvider.ProviderType);

                    var customParameterFilter = new NpgsqlParameter($"@{columnName}", sqlDbType);
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
                    var columnName = ParserName.Parse(columnFilter, "\"").Unquoted().Normalized().ToString();
                    var sqlDbType = (NpgsqlDbType)this.sqlDbMetadata.TryGetOwnerDbType(columnFilter.OriginalDbType, columnFilter.GetDbType(), false, false, columnFilter.MaxLength, tableFilter.OriginalProvider, NpgsqlSyncProvider.ProviderType);

                    // Add it as parameter
                    var sqlParamFilter = new NpgsqlParameter($"@{columnName}", sqlDbType);
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
                var filterTableName = ParserName.Parse(fullTableName, "\"").Quoted().Schema().ToString();

                var joinTableName = ParserName.Parse(customJoin.TableName, "\"").Quoted().Schema().ToString();

                var leftTableName = ParserName.Parse(customJoin.LeftTableName, "\"").Quoted().Schema().ToString();
                if (string.Equals(filterTableName, leftTableName, SyncGlobalization.DataSourceStringComparison))
                    leftTableName = "[base]";

                var rightTableName = ParserName.Parse(customJoin.RightTableName, "\"").Quoted().Schema().ToString();
                if (string.Equals(filterTableName, rightTableName, SyncGlobalization.DataSourceStringComparison))
                    rightTableName = "[base]";

                var leftColumName = ParserName.Parse(customJoin.LeftColumnName, "\"").Quoted().ToString();
                var rightColumName = ParserName.Parse(customJoin.RightColumnName, "\"").Quoted().ToString();

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

                var tableName = ParserName.Parse(tableFilter, "\"").Unquoted().ToString();
                if (string.Equals(tableName, filter.TableName, SyncGlobalization.DataSourceStringComparison))
                    tableName = "[base]";
                else
                    tableName = ParserName.Parse(tableFilter, "\"").Quoted().Schema().ToString();

                var columnName = ParserName.Parse(columnFilter, "\"").Quoted().ToString();
                var parameterName = ParserName.Parse(whereFilter.ParameterName, "\"").Unquoted().Normalized().ToString();
                var sqlDbType = (NpgsqlDbType)this.sqlDbMetadata.TryGetOwnerDbType(columnFilter.OriginalDbType, columnFilter.GetDbType(), false, false, columnFilter.MaxLength, tableFilter.OriginalProvider, NpgsqlSyncProvider.ProviderType);

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
        protected virtual NpgsqlCommand BuildSelectIncrementalChangesCommand(SyncFilter filter = null)
        {
            var sqlCommand = new NpgsqlCommand();
            var pTimestamp = new NpgsqlParameter("@sync_min_timestamp", NpgsqlDbType.Bigint) { Value = 0 };
            var pScopeId = new NpgsqlParameter("@sync_scope_id", NpgsqlDbType.Uuid) { Value = "NULL", IsNullable = true }; // <--- Ok THAT's Bad, but it's working :D

            sqlCommand.Parameters.Add(pTimestamp);
            sqlCommand.Parameters.Add(pScopeId);

            // Add filter parameters
            if (filter != null)
                CreateFilterParameters(sqlCommand, filter);

            var stringBuilder = new StringBuilder("SELECT DISTINCT");

            // ----------------------------------
            // Add all columns
            // ----------------------------------
            foreach (var pkColumn in this.tableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn, "\"").Quoted().ToString();
                stringBuilder.AppendLine($"\t[side].{columnName}, ");
            }
            foreach (var mutableColumn in this.tableDescription.GetMutableColumns())
            {
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
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
            foreach (var pkColumn in this.tableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn, "\"").Quoted().ToString();
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
        public async Task CreateSelectIncrementalChangesAsync(SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChanges).name;
            NpgsqlCommand cmdWithoutFilter() => BuildSelectIncrementalChangesCommand(null);
            await CreateProcedureCommandAsync(cmdWithoutFilter, commandName, connection, transaction).ConfigureAwait(false);

            if (filter != null)
            {
                commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChangesWithFilters, filter).name;
                NpgsqlCommand cmdWithFilter() => BuildSelectIncrementalChangesCommand(filter);
                await CreateProcedureCommandAsync(cmdWithFilter, commandName, connection, transaction).ConfigureAwait(false);
            }
        }

        public async Task DropSelectIncrementalChangesAsync(SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChanges).name;
            var commandText = $"DROP PROCEDURE {commandName};";

            using (var command = new NpgsqlCommand(commandText, (NpgsqlConnection)connection, (NpgsqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }

            if (filter != null)
            {
                var commandNameWithFilter = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChangesWithFilters, filter).name;
                var commandTextWithFilter = $"DROP PROCEDURE {commandNameWithFilter};";

                using (var command = new NpgsqlCommand(commandTextWithFilter, (NpgsqlConnection)connection, (NpgsqlTransaction)transaction))
                {
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }

        }

        //------------------------------------------------------------------
        // Select initialized changes command
        //------------------------------------------------------------------
        protected virtual NpgsqlCommand BuildSelectInitializedChangesCommand(SyncFilter filter = null)
        {
            var sqlCommand = new NpgsqlCommand();

            // Add filter parameters
            if (filter != null)
                CreateFilterParameters(sqlCommand, filter);

            var stringBuilder = new StringBuilder("SELECT DISTINCT");
            var columns = this.tableDescription.GetMutableColumns(false, true).ToList();
            for (var i = 0; i < columns.Count; i++)
            {
                var mutableColumn = columns[i];
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
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

        public async Task CreateSelectInitializedChangesAsync(SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectInitializedChanges).name;
            NpgsqlCommand cmdWithoutFilter() => BuildSelectInitializedChangesCommand(null);
            await CreateProcedureCommandAsync(cmdWithoutFilter, commandName, connection, transaction).ConfigureAwait(false);

            if (filter != null)
            {
                commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectInitializedChangesWithFilters, filter).name;
                NpgsqlCommand cmdWithFilter() => BuildSelectInitializedChangesCommand(filter);
                await CreateProcedureCommandAsync(cmdWithFilter, commandName, connection, transaction).ConfigureAwait(false);
            }
        }
        public async Task DropSelectInitializedChangesAsync(SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectInitializedChanges).name;
            var commandText = $"DROP PROCEDURE {commandName};";
            using (var command = new NpgsqlCommand(commandText, (NpgsqlConnection)connection, (NpgsqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }

            if (filter != null)
            {

                var commandNameWithFilter = this.sqlObjectNames.GetCommandName(DbCommandType.SelectInitializedChangesWithFilters, filter).name;
                var commandTextWithFilter = $"DROP PROCEDURE {commandNameWithFilter};";

                using (var command = new NpgsqlCommand(commandTextWithFilter, (NpgsqlConnection)connection, (NpgsqlTransaction)transaction))
                {
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }
        }

        public Task CreateBulkUpdateAsync(bool hasMutableColumns, DbConnection connection, DbTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public Task CreateBulkDeleteAsync(DbConnection connection, DbTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public Task DropBulkUpdateAsync(DbConnection connection, DbTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public Task DropBulkDeleteAsync(DbConnection connection, DbTransaction transaction)
        {
            throw new NotImplementedException();
        }

        private string CreateTVPTypeCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkTableType).name;
            stringBuilder.AppendLine($"CREATE TYPE {commandName} AS (");
            string str = "";
            foreach (var c in this.tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var isPrimaryKey = this.tableDescription.IsPrimaryKey(c.ColumnName);

                var columnName = ParserName.Parse(c, "\"").Quoted().ToString();

                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = this.sqlDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.GetDbType(), false, false, c.MaxLength, this.tableDescription.OriginalProvider, NpgsqlSyncProvider.ProviderType);
                var quotedColumnType = ParserName.Parse(sqlDbTypeString, "\"").Quoted().ToString();
                quotedColumnType += this.sqlDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.GetDbType(), false, false, c.MaxLength, c.Precision, c.Scale, this.tableDescription.OriginalProvider, NpgsqlSyncProvider.ProviderType);

                stringBuilder.AppendLine($"{str}{columnName} {quotedColumnType} ");
                str = ", ";
            }

            stringBuilder.Append(");");
            return stringBuilder.ToString();
        }

        public async Task CreateTVPTypeAsync(DbConnection connection, DbTransaction transaction)
        {
            using (NpgsqlCommand sqlCommand = new NpgsqlCommand(this.CreateTVPTypeCommandText(), (NpgsqlConnection)connection, (NpgsqlTransaction)transaction))
            {

                await sqlCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        public Task DropTVPTypeAsync(DbConnection connection, DbTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public Task<DbCommand> GetExistsStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
        public Task<DbCommand> GetExistsBulkTypeCommandAsync(DbBulkType bulkType, DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
        public Task<DbCommand> GetCreateBulkTypeCommandAsync(DbBulkType bulkType, DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
        public Task<DbCommand> GetCreateStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, bool overwrite, DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
        public Task<DbCommand> GetDropStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
    }
}