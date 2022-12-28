using Dotmim.Sync.Builders;
using Dotmim.Sync.PostgreSql.Scope;
using Dotmim.Sync.PostgreSql.Builders;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using static Npgsql.Replication.PgOutput.Messages.RelationMessage;

namespace Dotmim.Sync.PostgreSql.Builders
{
    public class NpgsqlBuilderProcedure
    {
        internal const string NPGSQL_PREFIX_PARAMETER = "in_";
        private string scopeName;
        private SyncSetup setup;
        private SyncTable tableDescription;
        private ParserName tableName;
        private ParserName trackingTableName;
        public NpgsqlBuilderProcedure(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
        {
            this.tableDescription = tableDescription;
            this.tableName = tableName;
            this.trackingTableName = trackingTableName;
            this.setup = setup;
            this.scopeName = scopeName;
            this.NpgsqlObjectNames = new NpgsqlObjectNames(this.tableDescription, tableName, trackingTableName, setup, scopeName);
            this.NpgsqlDbMetadata = new NpgsqlDbMetadata();
        }

        public NpgsqlDbMetadata NpgsqlDbMetadata { get; set; }
        public NpgsqlObjectNames NpgsqlObjectNames { get; set; }


        public DbCommand CreateResetCommand(DbConnection connection, DbTransaction transaction)
        {

            var procName = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.Reset);
            var procNameQuoted = ParserName.Parse(procName, "\"").Quoted().ToString();

            var tableQuoted = ParserName.Parse(tableName.ToString(), "\"").Quoted().ToString();
            var tableUnquoted = tableName.Unquoted().ToString();
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);


            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = (NpgsqlConnection)connection;
            cmd.Transaction = (NpgsqlTransaction)transaction;

            NpgsqlParameter sqlParameter2 = new NpgsqlParameter(@"sync_row_count", NpgsqlDbType.Integer)
            {
                Direction = ParameterDirection.Output
            };
            cmd.Parameters.Add(sqlParameter2);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE OR REPLACE FUNCTION {schema}.{procNameQuoted} (");
            string str = "\t";
            foreach (NpgsqlParameter parameter in cmd.Parameters)
            {
                stringBuilder.Append(string.Concat(str, CreateParameterDeclaration(parameter)));
                str = ",\n\t";
            }
            stringBuilder.AppendLine("\n) ");
            stringBuilder.AppendLine("AS $BODY$ ");
            stringBuilder.AppendLine("BEGIN");
            stringBuilder.AppendLine($"ALTER TABLE {schema}.{tableQuoted} DISABLE TRIGGER ALL;");
            stringBuilder.AppendLine($"DELETE FROM {schema}.{tableQuoted};");
            stringBuilder.AppendLine($"DELETE FROM {schema}.{tableQuoted};");
            stringBuilder.AppendLine($"ALTER TABLE {schema}.{tableQuoted} ENABLE TRIGGER ALL;");
            stringBuilder.AppendLine(string.Concat(@"GET DIAGNOSTICS ""sync_row_count"" = ROW_COUNT;"));
            stringBuilder.AppendLine("END;");
            stringBuilder.AppendLine("$BODY$ LANGUAGE 'plpgsql';");
            cmd.Parameters.Clear();
            cmd.CommandText = stringBuilder.ToString();
            return cmd;
        }

        public Task<DbCommand> GetCreateStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            var command = storedProcedureType switch
            {
                DbStoredProcedureType.SelectChanges => this.CreateSelectIncrementalChangesCommand(connection, transaction),
                DbStoredProcedureType.SelectChangesWithFilters => this.CreateSelectIncrementalChangesCommand(connection, transaction, filter),
                DbStoredProcedureType.SelectInitializedChanges => this.CreateSelectInitializedChangesCommand(connection, transaction),
                DbStoredProcedureType.SelectInitializedChangesWithFilters => this.CreateSelectInitializedChangesCommand(connection, transaction, filter),
                DbStoredProcedureType.SelectRow => this.CreateSelectRowCommand(connection, transaction),
                DbStoredProcedureType.UpdateRow => this.CreateUpdateCommand(connection, transaction),
                DbStoredProcedureType.DeleteRow => this.CreateDeleteCommand(connection, transaction),
                DbStoredProcedureType.DeleteMetadata => this.CreateDeleteMetadataCommand(connection, transaction),
                DbStoredProcedureType.Reset => this.CreateResetCommand(connection, transaction),
                _ => null,
            };

            return Task.FromResult(command);
        }

        public Task<DbCommand> GetDropStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {

            if (filter == null && (storedProcedureType == DbStoredProcedureType.SelectChangesWithFilters || storedProcedureType == DbStoredProcedureType.SelectInitializedChangesWithFilters))
                return Task.FromResult<DbCommand>(null);

            if (storedProcedureType == DbStoredProcedureType.BulkDeleteRows ||
                storedProcedureType == DbStoredProcedureType.BulkUpdateRows || storedProcedureType == DbStoredProcedureType.BulkTableType)
                return Task.FromResult<DbCommand>(null);

            var commandName = this.NpgsqlObjectNames.GetStoredProcedureCommandName(storedProcedureType, filter);

            if (string.IsNullOrEmpty(commandName))
                return Task.FromResult<DbCommand>(null);

            var text = $"DROP FUNCTION {commandName};";

            //if (storedProcedureType == DbStoredProcedureType.BulkTableType)
            //    text = $"DROP TYPE {commandName};";

            var sqlCommand = connection.CreateCommand();

            sqlCommand.Transaction = transaction;

            sqlCommand.CommandText = text;

            return Task.FromResult(sqlCommand);
        }

        public Task<DbCommand> GetExistsStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            if (filter == null && (storedProcedureType == DbStoredProcedureType.SelectChangesWithFilters || storedProcedureType == DbStoredProcedureType.SelectInitializedChangesWithFilters))
                return Task.FromResult<DbCommand>(null);

            // Todo: will check it later
            if (storedProcedureType == DbStoredProcedureType.BulkDeleteRows ||
                storedProcedureType == DbStoredProcedureType.BulkUpdateRows || storedProcedureType == DbStoredProcedureType.BulkTableType)
                return Task.FromResult<DbCommand>(null);

            var quotedProcedureName = this.NpgsqlObjectNames.GetStoredProcedureCommandName(storedProcedureType, filter);
            
            // Todo: will check it later
            if (string.IsNullOrEmpty(quotedProcedureName))
                return Task.FromResult<DbCommand>(null);

            var procedureName = ParserName.Parse(quotedProcedureName, "\"").ToString();

            var text = @"SELECT count(*)
                          FROM pg_catalog.pg_proc as pc
                          JOIN pg_namespace as n on pc.pronamespace = n.oid
                          WHERE nspname = @schemaName and proname = @name";


            var sqlCommand = connection.CreateCommand();

            sqlCommand.Transaction = transaction;

            sqlCommand.CommandText = text;

            var p = sqlCommand.CreateParameter();
            p.ParameterName = "@name";
            p.Value = procedureName;
            sqlCommand.Parameters.Add(p);

            p = sqlCommand.CreateParameter();
            p.ParameterName = "@schemaName";
            p.Value = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(ParserName.Parse(quotedProcedureName));
            sqlCommand.Parameters.Add(p);

            return Task.FromResult(sqlCommand);
        }
        protected void AddPkColumnParametersToCommand(NpgsqlCommand sqlCommand)
        {
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
                sqlCommand.Parameters.Add(GetSqlParameter(pkColumn));
        }

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
                    tableName = @"""base""";
                else
                    tableName = ParserName.Parse(tableFilter, "\"").Quoted().ToString();

                var columnName = ParserName.Parse(columnFilter, "\"").Quoted().ToString();
                var parameterName = ParserName.Parse(whereFilter.ParameterName, "\"").Unquoted().Normalized().ToString();

                var param = filter.Parameters[parameterName];

                if (param == null)
                    throw new FilterParamColumnNotExistsException(columnName, whereFilter.TableName);

                stringBuilder.Append($"{and2}({tableName}.{columnName} = \"{NPGSQL_PREFIX_PARAMETER}{parameterName}\"");

                if (param.AllowNull)
                    stringBuilder.Append($" OR \"in_{parameterName}\" IS NULL");

                stringBuilder.Append($")");

                and2 = " AND ";

            }
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"  )");

            if (checkTombstoneRows)
            {
                stringBuilder.AppendLine($" OR side.sync_row_is_tombstone = TRUE");
                stringBuilder.AppendLine($")");
            }
            // Managing when state is tombstone


            return stringBuilder.ToString();
        }

        protected string CreateParameterDeclaration(NpgsqlParameter param)
        {
            var stringBuilder = new StringBuilder();

            var tmpColumn = new SyncColumn(param.ParameterName)
            {
                OriginalDbType = param.NpgsqlDbType.ToString(),
                OriginalTypeName = param.NpgsqlDbType.ToString().ToLowerInvariant(),
                MaxLength = param.Size,
                Precision = param.Precision,
                Scale = param.Scale,
                DbType = (int)param.DbType,
                ExtraProperty1 = string.IsNullOrEmpty(param.SourceColumn) ? null : param.SourceColumn
            };

            var columnDeclarationString = this.NpgsqlDbMetadata.GetCompatibleColumnTypeDeclarationString(tmpColumn, this.tableDescription.OriginalProvider);

            stringBuilder.Append($"{param.ParameterName} {columnDeclarationString}");
            if (param.Value != null)
                stringBuilder.Append($" = {param.Value}");
            else if (param.Direction == ParameterDirection.Input)
                stringBuilder.Append(" = NULL");

            var outstr = new StringBuilder("out ");
            if (param.Direction == ParameterDirection.Output || param.Direction == ParameterDirection.InputOutput)
                stringBuilder = outstr.Append(stringBuilder);

            return stringBuilder.ToString();
        }

        protected NpgsqlParameter GetSqlParameter(SyncColumn column)
        {
            var paramName = $"{NPGSQL_PREFIX_PARAMETER}{ParserName.Parse(column).Unquoted().Normalized().ToString()}";
            var paramNameQuoted = ParserName.Parse(paramName, "\"").Quoted().ToString();
            var sqlParameter = new NpgsqlParameter
            {
                ParameterName = paramNameQuoted
            };

            // Get the good SqlDbType (even if we are not from Sql Server def)
            var sqlDbType = this.tableDescription.OriginalProvider == NpgsqlSyncProvider.ProviderType ?
                this.NpgsqlDbMetadata.GetNpgsqlDbType(column) : this.NpgsqlDbMetadata.GetOwnerDbTypeFromDbType(column);


            sqlParameter.NpgsqlDbType = sqlDbType;
            sqlParameter.IsNullable = column.AllowDBNull;

            var (p, s) = this.NpgsqlDbMetadata.GetCompatibleColumnPrecisionAndScale(column, this.tableDescription.OriginalProvider);

            if (p > 0)
            {
                sqlParameter.Precision = p;
                if (s > 0)
                    sqlParameter.Scale = s;
            }

            var m = this.NpgsqlDbMetadata.GetCompatibleMaxLength(column, this.tableDescription.OriginalProvider);

            if (m > 0)
                sqlParameter.Size = m;

            return sqlParameter;
        }

        private void AddColumnParametersToCommand(NpgsqlCommand sqlCommand)
        {
            foreach (var column in this.tableDescription.Columns.Where(c => !c.IsReadOnly))
                sqlCommand.Parameters.Add(GetSqlParameter(column));
        }


        private DbCommand CreateDeleteCommand(DbConnection connection, DbTransaction transaction)
        {
            var procName = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.DeleteRow);
            var procNameQuoted = ParserName.Parse(procName, "\"").Quoted().ToString();

            var trackingTableQuoted = ParserName.Parse(trackingTableName.ToString(), "\"").Quoted().ToString();
            var tableQuoted = ParserName.Parse(tableName.ToString(), "\"").Quoted().ToString();
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);

            var sqlCommand = new NpgsqlCommand();
            sqlCommand.Connection = (NpgsqlConnection)connection;
            sqlCommand.Transaction = (NpgsqlTransaction)transaction;

            this.AddPkColumnParametersToCommand(sqlCommand);

            var sqlParameter0 = new NpgsqlParameter(@"sync_scope_id", NpgsqlDbType.Uuid);
            sqlCommand.Parameters.Add(sqlParameter0);

            var sqlParameter = new NpgsqlParameter(@"sync_force_write", NpgsqlDbType.Bigint);
            sqlCommand.Parameters.Add(sqlParameter);

            var sqlParameter1 = new NpgsqlParameter(@"sync_min_timestamp", NpgsqlDbType.Bigint);
            sqlCommand.Parameters.Add(sqlParameter1);

            var sqlParameter2 = new NpgsqlParameter(@"sync_row_count", NpgsqlDbType.Integer)
            {
                Direction = ParameterDirection.Output
            };
            sqlCommand.Parameters.Add(sqlParameter2);

            string str6 = NpgsqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "t", "side");

            var stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"CREATE OR REPLACE FUNCTION {schema}.{procNameQuoted}(");
            string str = "\t";
            foreach (NpgsqlParameter parameter in sqlCommand.Parameters)
            {
                stringBuilder.Append(string.Concat(str, CreateParameterDeclaration(parameter)));
                str = ",\n\t";
            }
            stringBuilder.AppendLine(") ");

            stringBuilder.AppendLine("AS $BODY$ ");
            stringBuilder.AppendLine("BEGIN");

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WITH dms_changed as ( ");
            stringBuilder.AppendLine($"DELETE from {schema}.{tableQuoted} base");
            stringBuilder.Append($"USING {schema}.{trackingTableQuoted} side ");
            stringBuilder.AppendLine(@$"WHERE {NpgsqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "base", "side")} ");
            stringBuilder.AppendLine("AND (side.timestamp <= sync_min_timestamp OR side.timestamp IS NULL OR side.update_scope_id = sync_scope_id OR sync_force_write = 1)");
            stringBuilder.Append("AND ");
            stringBuilder.AppendLine(string.Concat("(", NpgsqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKeys, "base"), ") returning "));
            string comma = "";
            foreach (var primaryKeyColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(primaryKeyColumn, "\"").Quoted().ToString();
                stringBuilder.Append($"{comma} base.{columnName}");
                comma = ", ";
            }

            stringBuilder.AppendLine(" ) ");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"UPDATE {schema}.{trackingTableQuoted} SET");
            stringBuilder.AppendLine($"\t\"update_scope_id\" = \"sync_scope_id\",");
            stringBuilder.AppendLine($"\t\"sync_row_is_tombstone\" = TRUE,");
            stringBuilder.AppendLine($"    \"timestamp\" = {NpgsqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine($"\t\"last_change_datetime\" = now()");
            stringBuilder.AppendLine($"FROM {schema}.{trackingTableQuoted} side");
            stringBuilder.AppendLine($"JOIN dms_changed t on {str6};");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("GET DIAGNOSTICS ", sqlParameter2.ParameterName, " = ROW_COUNT;"));
            stringBuilder.AppendLine("END; $BODY$ LANGUAGE 'plpgsql';");
            sqlCommand.Parameters.Clear();
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        private DbCommand CreateDeleteMetadataCommand(DbConnection connection, DbTransaction transaction)
        {
            var procName = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.DeleteMetadata);
            var procNameQuoted = ParserName.Parse(procName, "\"").Quoted().ToString();

            var trackingTableQuoted = ParserName.Parse(trackingTableName.ToString(), "\"").Quoted().ToString();
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);

            NpgsqlCommand sqlCommand = new NpgsqlCommand();
            sqlCommand.Connection = (NpgsqlConnection)connection;
            sqlCommand.Transaction = (NpgsqlTransaction)transaction;
            this.AddPkColumnParametersToCommand(sqlCommand);
            NpgsqlParameter sqlParameter1 = new NpgsqlParameter(@"sync_row_timestamp", NpgsqlDbType.Bigint);
            sqlCommand.Parameters.Add(sqlParameter1);
            NpgsqlParameter sqlParameter2 = new NpgsqlParameter(@"sync_row_count", NpgsqlDbType.Integer)
            {
                Direction = ParameterDirection.Output
            };
            sqlCommand.Parameters.Add(sqlParameter2);
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"create or replace function {schema}.{procNameQuoted}(");
            string str = "\t";
            foreach (NpgsqlParameter parameter in sqlCommand.Parameters)
            {
                stringBuilder.Append(string.Concat(str, CreateParameterDeclaration(parameter)));
                str = ",\n\t";
            }
            stringBuilder.AppendLine(") ");

            stringBuilder.AppendLine("AS $BODY$ ");
            stringBuilder.AppendLine("BEGIN");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"DELETE FROM {schema}.{trackingTableQuoted} side");
            stringBuilder.AppendLine(@$"WHERE side.""timestamp"" < {sqlParameter1.ParameterName};");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("GET DIAGNOSTICS ", sqlParameter2.ParameterName, " = ROW_COUNT;"));
            stringBuilder.AppendLine("END;");
            stringBuilder.AppendLine("$BODY$ LANGUAGE 'plpgsql';");
            sqlCommand.Parameters.Clear();
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        private string CreateFilterCustomJoins(SyncFilter filter)
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
                    leftTableName = "base";

                var rightTableName = ParserName.Parse(customJoin.RightTableName, "\"").Quoted().Schema().ToString();
                if (string.Equals(filterTableName, rightTableName, SyncGlobalization.DataSourceStringComparison))
                    rightTableName = "base";

                var leftColumName = ParserName.Parse(customJoin.LeftColumnName, "\"").Quoted().ToString();
                var rightColumName = ParserName.Parse(customJoin.RightColumnName, "\"").Quoted().ToString();

                stringBuilder.AppendLine($"{joinTableName} ON {leftTableName}.{leftColumName} = {rightTableName}.{rightColumName}");
            }

            return stringBuilder.ToString();
        }

        private void CreateFilterParameters(NpgsqlCommand sqlCommand, SyncFilter filter)
        {
            var parameters = filter.Parameters;

            if (parameters.Count == 0)
                return;

            foreach (var param in parameters)
            {
                if (param.DbType.HasValue)
                {
                    // Get column name and type
                    var columnName = ParserName.Parse(param.Name, "\"").Quoted().Normalized().ToString();
                    var sqlDbType = this.NpgsqlDbMetadata.GetOwnerDbTypeFromDbType(new SyncColumn(columnName) { DbType = (int)param.DbType });

                    var customParameterFilter = new NpgsqlParameter($"{columnName}", sqlDbType);
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
                    var columnName = ParserName.Parse(columnFilter, "\"").Quoted().Normalized().ToString();
                    //var sqlDbType = (NpgsqlDbType)this.NpgsqlDbMetadata.TryGetOwnerDbType(columnFilter.OriginalDbType, columnFilter.GetDbType(), false, false, columnFilter.MaxLength, tableFilter.OriginalProvider, NpgsqlSyncProvider.ProviderType);
                    var sqlDbType = tableFilter.OriginalProvider == NpgsqlSyncProvider.ProviderType ? this.NpgsqlDbMetadata.GetNpgsqlDbType(columnFilter) : this.NpgsqlDbMetadata.GetOwnerDbTypeFromDbType(columnFilter);

                    // Add it as parameter
                    var sqlParamFilter = new NpgsqlParameter($"{columnName}", sqlDbType);
                    sqlParamFilter.Size = columnFilter.MaxLength;
                    sqlParamFilter.IsNullable = param.AllowNull;
                    sqlParamFilter.Value = param.DefaultValue;
                    sqlCommand.Parameters.Add(sqlParamFilter);
                }

            }
        }

        private DbCommand CreateSelectIncrementalChangesCommand(DbConnection connection, DbTransaction transaction, SyncFilter filter = null)
        {

            string procName = string.Empty;
            if (filter != null)
                procName = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectChangesWithFilters, filter);
            else
                procName = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectChanges);

            var procNameQuoted = ParserName.Parse(procName, "\"").Quoted().ToString();

            var trackingTableQuoted = ParserName.Parse(trackingTableName.ToString(), "\"").Quoted().ToString();
            var tableQuoted = ParserName.Parse(tableName.ToString(), "\"").Quoted().ToString();
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);

            var sqlCommand = new NpgsqlCommand();
            sqlCommand.Connection = (NpgsqlConnection)connection;
            sqlCommand.Transaction = (NpgsqlTransaction)transaction;

            // Add filter parameters
            if (filter != null)
                CreateFilterParameters(sqlCommand, filter);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE OR REPLACE FUNCTION {schema}.{procNameQuoted} (");
            string str = "\t,";
            stringBuilder.AppendLine("\"sync_min_timestamp\" bigint = NULL, ");
            stringBuilder.AppendLine("\"sync_scope_id\" uuid = NULL ");
            foreach (NpgsqlParameter parameter in sqlCommand.Parameters)
            {
                parameter.ParameterName = $@"""{NPGSQL_PREFIX_PARAMETER}{ParserName.Parse(parameter.ParameterName, "\"").Unquoted().ToString()}""";
                stringBuilder.Append(string.Concat(str, CreateParameterDeclaration(parameter)));
                str = ",\n\t";
            }
            stringBuilder.AppendLine("\n) ");

            var pTimestamp = new NpgsqlParameter(@"sync_min_timestamp", NpgsqlDbType.Bigint) { Value = 0 };
            var pScopeId = new NpgsqlParameter(@"sync_scope_id", NpgsqlDbType.Uuid) { Value = "NULL", IsNullable = true }; // <--- Ok THAT's Bad, but it's working :D
            sqlCommand.Parameters.Add(pTimestamp);
            sqlCommand.Parameters.Add(pScopeId);

            stringBuilder.AppendLine("RETURNS TABLE ( ");
            foreach (var mutableColumn in this.tableDescription.GetMutableColumns(false, true))
            {
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                //var dataType = NpgsqlDbMetadata.GetOwnerDbTypeFromDbType(mutableColumn).ToString().ToLowerInvariant();
                var columnType = this.NpgsqlDbMetadata.GetCompatibleColumnTypeDeclarationString(mutableColumn, this.tableDescription.OriginalProvider);
                stringBuilder.AppendLine($"\t{columnName} {columnType}, ");
            }
            stringBuilder.AppendLine($"\t\"sync_row_is_tombstone\" boolean, ");
            stringBuilder.AppendLine($"\t\"sync_update_scope_id\" uuid");
            stringBuilder.AppendLine($") ");
            stringBuilder.AppendLine("AS $BODY$ ");
            stringBuilder.AppendLine("BEGIN");
            if (filter != null)
                stringBuilder.AppendLine("RETURN QUERY SELECT DISTINCT");
            else
                stringBuilder.AppendLine("RETURN QUERY SELECT");
            // ----------------------------------
            // Add all columns
            // ----------------------------------
            foreach (var pkColumn in this.tableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn, "\"").Quoted().ToString();
                stringBuilder.AppendLine($"\tside.{columnName}, ");
            }
            foreach (var mutableColumn in this.tableDescription.GetMutableColumns())
            {
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                stringBuilder.AppendLine($"\tbase.{columnName}, ");
            }
            stringBuilder.AppendLine($"\tside.\"sync_row_is_tombstone\", ");
            stringBuilder.AppendLine($"\tside.\"update_scope_id\" ");
            // ----------------------------------
            stringBuilder.AppendLine($"FROM {schema}.{tableQuoted} base");

            // ----------------------------------
            // Make Right Join
            // ----------------------------------
            stringBuilder.Append($"RIGHT JOIN {schema}.{trackingTableQuoted} side ON ");

            string empty = "";
            foreach (var pkColumn in this.tableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn, "\"").Quoted().ToString();
                stringBuilder.Append($"{empty}base.{columnName} = side.{columnName}");
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


            stringBuilder.AppendLine("\tside.\"timestamp\" > \"sync_min_timestamp\"");
            stringBuilder.AppendLine("\tAND (side.\"update_scope_id\" <> \"sync_scope_id\" OR side.\"update_scope_id\" IS NULL)");
            stringBuilder.AppendLine(");");
            stringBuilder.AppendLine("END; $BODY$ LANGUAGE 'plpgsql';");
            sqlCommand.Parameters.Clear();

            sqlCommand.CommandText = stringBuilder.ToString();

            return sqlCommand;
        }


        private DbCommand CreateSelectInitializedChangesCommand(DbConnection connection, DbTransaction transaction, SyncFilter filter = null)
        {
            string procName = string.Empty;
            if (filter != null)
                procName = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectInitializedChangesWithFilters, filter);
            else
                procName = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectInitializedChanges);

            var procNameQuoted = ParserName.Parse(procName, "\"").Quoted().ToString();

            var trackingTableQuoted = ParserName.Parse(trackingTableName.ToString(), "\"").Quoted().ToString();
            var tableQuoted = ParserName.Parse(tableName.ToString(), "\"").Quoted().ToString();
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);

            var command = new NpgsqlCommand
            {
                CommandTimeout = 0
            };
            command.Connection = (NpgsqlConnection)connection;
            command.Transaction = (NpgsqlTransaction)transaction;


            

            // Add filter parameters
            if (filter != null)
                this.CreateFilterParameters(command, filter);

            var columns = this.tableDescription.GetMutableColumns(false, true).ToList();
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE OR REPLACE FUNCTION {schema}.{procNameQuoted} (");
            string str = "\t,";
            stringBuilder.AppendLine("sync_min_timestamp bigint = NULL, ");
            stringBuilder.AppendLine("sync_scope_id uuid = NULL ");


            foreach (NpgsqlParameter parameter in command.Parameters)
            {
                parameter.ParameterName = $@"""in_{ParserName.Parse(parameter.ParameterName, "\"").Unquoted().ToString()}""";
                stringBuilder.Append(string.Concat(str, CreateParameterDeclaration(parameter)));
                str = ",\n\t";
            }
            stringBuilder.AppendLine(") ");


            var pTimestamp = new NpgsqlParameter(@"sync_min_timestamp", NpgsqlDbType.Bigint);
            var pScopeId = new NpgsqlParameter(@"sync_scope_id", NpgsqlDbType.Uuid) { Value = "NULL", IsNullable = true };
            command.Parameters.Add(pTimestamp);
            command.Parameters.Add(pScopeId);

            stringBuilder.AppendLine("RETURNS TABLE ( ");
            string str2 = "";
            foreach (var mutableColumn in this.tableDescription.GetMutableColumns(false, true))
            {
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                //var dataType = NpgsqlDbMetadata.GetOwnerDbTypeFromDbType(mutableColumn).ToString().ToLowerInvariant();
                var columnType = this.NpgsqlDbMetadata.GetCompatibleColumnTypeDeclarationString(mutableColumn, this.tableDescription.OriginalProvider);

                stringBuilder.AppendLine($"\t{str2}{columnName} {columnType} ");
                str2 = ",";
            }
            stringBuilder.AppendLine($") ");
            stringBuilder.AppendLine("AS $BODY$ ");
            stringBuilder.AppendLine("BEGIN");



            // if we have a filter we may have joins that will duplicate lines
            if (filter != null)
                stringBuilder.AppendLine("RETURN QUERY SELECT DISTINCT");
            else
                stringBuilder.AppendLine("RETURN QUERY SELECT");


            for (var i = 0; i < columns.Count; i++)
            {
                var mutableColumn = columns[i];
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                stringBuilder.Append($"\tbase.{columnName}");

                if (i < columns.Count - 1)
                    stringBuilder.AppendLine(", ");
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"FROM {schema}.{tableQuoted} base");

            // ----------------------------------
            // Make Left Join
            // ----------------------------------
            stringBuilder.Append($"LEFT JOIN {schema}.{trackingTableQuoted} side ON ");

            string empty = "";
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn, "\"").Quoted().ToString();
                stringBuilder.Append($"{empty}base.{columnName} = side.{columnName}");
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
                var createFilterWhereSide = CreateFilterWhereSide(filter);
                stringBuilder.Append(createFilterWhereSide);

                if (!string.IsNullOrEmpty(createFilterWhereSide))
                    stringBuilder.AppendLine($"AND ");

                var createFilterCustomWheres = CreateFilterCustomWheres(filter);
                stringBuilder.Append(createFilterCustomWheres);

                if (!string.IsNullOrEmpty(createFilterCustomWheres))
                    stringBuilder.AppendLine($"AND ");
            }
            // ----------------------------------


            stringBuilder.AppendLine("\t(side.\"timestamp\" > \"sync_min_timestamp\" OR  \"sync_min_timestamp\" IS NULL)");
            stringBuilder.AppendLine(");");
            stringBuilder.AppendLine("END;");
            stringBuilder.AppendLine("$BODY$ LANGUAGE 'plpgsql';");
            command.Parameters.Clear();
            command.CommandText = stringBuilder.ToString();

            return command;
        }

        private DbCommand CreateSelectRowCommand(DbConnection connection, DbTransaction transaction)
        {
            var procName = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectRow);
            var procNameQuoted = ParserName.Parse(procName, "\"").Quoted().ToString();

            var trackingTableQuoted = ParserName.Parse(trackingTableName.ToString(), "\"").Quoted().ToString();
            var tableQuoted = ParserName.Parse(tableName.ToString(), "\"").Quoted().ToString();
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);

            var cmd = new NpgsqlCommand();
            cmd.Connection = (NpgsqlConnection)connection;
            cmd.Transaction = (NpgsqlTransaction)transaction;
            this.AddPkColumnParametersToCommand(cmd);
            NpgsqlParameter sqlParameter = new NpgsqlParameter(@"sync_scope_id", NpgsqlDbType.Uuid);

            cmd.Parameters.Add(sqlParameter);

            var stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"CREATE OR REPLACE FUNCTION {schema}.{procNameQuoted} (");
            string str = "\t";
            foreach (NpgsqlParameter parameter in cmd.Parameters)
            {
                stringBuilder.Append(string.Concat(str, CreateParameterDeclaration(parameter)));
                str = ",\n\t";
            }
            stringBuilder.AppendLine("\n) ");

            stringBuilder.AppendLine("RETURNS TABLE ( ");
            foreach (var mutableColumn in this.tableDescription.GetMutableColumns(false, true))
            {
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                //var dataType = NpgsqlDbMetadata.GetOwnerDbTypeFromDbType(mutableColumn).ToString().ToLowerInvariant();
                var columnType = this.NpgsqlDbMetadata.GetCompatibleColumnTypeDeclarationString(mutableColumn, this.tableDescription.OriginalProvider);

                stringBuilder.AppendLine($"\t{columnName} {columnType}, ");
            }
            stringBuilder.AppendLine($"\t\"sync_row_is_tombstone\" boolean, ");
            stringBuilder.AppendLine($"\t\"update_scope_id\" uuid");
            stringBuilder.AppendLine($") ");
            stringBuilder.AppendLine($"LANGUAGE plpgsql AS $BODY$");
            stringBuilder.AppendLine($"BEGIN");
            stringBuilder.AppendLine("RETURN QUERY SELECT ");
            var stringBuilder1 = new StringBuilder();
            string empty = string.Empty;
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn, "\"").Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();

                stringBuilder1.Append($@"{empty}side.{columnName} = ""{NPGSQL_PREFIX_PARAMETER}{parameterName}""");
                empty = " AND ";
            }
            foreach (var mutableColumn in this.tableDescription.GetMutableColumns(false, true))
            {

                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                var isPrimaryKey = this.tableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.AppendLine($"\tside.{columnName}, ");
                else
                    stringBuilder.AppendLine($"\tbase.{columnName}, ");
            }
            stringBuilder.AppendLine($"\tside.\"sync_row_is_tombstone\" as sync_row_is_tombstone, ");
            stringBuilder.AppendLine($"\tside.\"update_scope_id\" as sync_update_scope_id");
            stringBuilder.AppendLine($"FROM {schema}.{tableQuoted} base");
            stringBuilder.AppendLine($"RIGHT JOIN {schema}.{trackingTableQuoted} side ON");

            string str2 = string.Empty;
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn, "\"").Quoted().ToString();
                stringBuilder.Append($"{str2}base.{columnName} = side.{columnName} ");
                str2 = " AND ";
            }
            //stringBuilder.AppendLine();
            stringBuilder.Append(string.Concat("WHERE ", stringBuilder1.ToString(), ";"));
            stringBuilder.Append("END; $BODY$;");
            cmd.Parameters.Clear();
            cmd.CommandText = stringBuilder.ToString();
            return cmd;
        }
        private DbCommand CreateUpdateCommand(DbConnection connection, DbTransaction transaction)
        {
            var hasMutableColumns = this.tableDescription.GetMutableColumns(false).Any();
            var procName = this.NpgsqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.UpdateRow);
            var procNameQuoted = ParserName.Parse(procName, "\"").Quoted().ToString();

            var trackingTableQuoted = ParserName.Parse(trackingTableName.ToString(), "\"").Quoted().ToString();
            var tableQuoted = ParserName.Parse(tableName.ToString(), "\"").Quoted().ToString();
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);

            var sqlCommand = new NpgsqlCommand();
            sqlCommand.Connection = (NpgsqlConnection)connection;
            sqlCommand.Transaction = (NpgsqlTransaction)transaction;
            var stringBuilder = new StringBuilder();

            this.AddColumnParametersToCommand(sqlCommand);

            var sqlParameter2 = new NpgsqlParameter("sync_scope_id", NpgsqlDbType.Uuid);
            sqlCommand.Parameters.Add(sqlParameter2);

            var sqlParameter3 = new NpgsqlParameter("sync_force_write", NpgsqlDbType.Bigint);
            sqlCommand.Parameters.Add(sqlParameter3);

            var sqlParameter1 = new NpgsqlParameter("sync_min_timestamp", NpgsqlDbType.Bigint);
            sqlCommand.Parameters.Add(sqlParameter1);

            var sqlParameter4 = new NpgsqlParameter("sync_row_count", NpgsqlDbType.Integer)
            {
                Direction = ParameterDirection.Output
            };
            sqlCommand.Parameters.Add(sqlParameter4);

            var listColumnsTmp = new StringBuilder();
            var listColumnsTmp2 = new StringBuilder();
            var listColumnsTmp3 = new StringBuilder();

            stringBuilder.AppendLine($"CREATE OR REPLACE FUNCTION {schema}.{procNameQuoted} (");
            string str = "\t";
            foreach (NpgsqlParameter parameter in sqlCommand.Parameters)
            {
                stringBuilder.Append(string.Concat(str, CreateParameterDeclaration(parameter)));
                str = ",\n\t";
            }
            stringBuilder.AppendLine(") \n\t");
            stringBuilder.AppendLine("AS $BODY$");
            var and = "";
            foreach (var column in this.tableDescription.GetPrimaryKeysColumns())
            {
                var param = GetSqlParameter(column);
                var parameterwithoutquotes = ParserName.Parse(column).Unquoted().ToString();

                param.ParameterName = $"t_in_{parameterwithoutquotes}";
                var declar = CreateParameterDeclaration(param);
                var columnNameQuoted = ParserName.Parse(column, "\"").Quoted().ToString();

                var parameterNameQuoted = ParserName.Parse(param.ParameterName).Unquoted().ToString();

                // Primary keys column name, with quote
                listColumnsTmp.Append($"{columnNameQuoted}, ");

                // param name without type
                listColumnsTmp2.Append($"{parameterNameQuoted}, ");

                // param name with type
                stringBuilder.AppendLine($"DECLARE {declar};");

                // Param equal IS NULL
                listColumnsTmp3.Append($"{and}{parameterNameQuoted} IS NULL");

                and = " AND ";

            }



            stringBuilder.AppendLine("DECLARE ts bigint;");
            stringBuilder.AppendLine("DECLARE t_update_scope_id uuid;");
            stringBuilder.AppendLine("BEGIN");
            stringBuilder.AppendLine("ts := 0;");
            stringBuilder.AppendLine($"SELECT {listColumnsTmp.ToString()}");
            stringBuilder.AppendLine($@"timestamp, update_scope_id FROM {schema}.{trackingTableQuoted} ");
            stringBuilder.AppendLine($"WHERE {NpgsqlManagementUtils.WhereColumnAndParameters(this.tableDescription.GetPrimaryKeysColumns(), trackingTableQuoted)} LIMIT 1 ");
            stringBuilder.AppendLine($@"INTO {listColumnsTmp2.ToString()} ts, t_update_scope_id;");
            stringBuilder.AppendLine();

            if (hasMutableColumns)
            {
                stringBuilder.AppendLine($"UPDATE {schema}.{tableQuoted}");
                stringBuilder.Append($"SET {NpgsqlManagementUtils.CommaSeparatedUpdateFromParameters(this.tableDescription)}");
                stringBuilder.Append($"WHERE {NpgsqlManagementUtils.WhereColumnAndParameters(this.tableDescription.GetPrimaryKeysColumns(), "")}");
                stringBuilder.AppendLine($" AND (ts <= sync_min_timestamp OR ts IS NULL OR t_update_scope_id  = sync_scope_id OR sync_force_write = 1);");
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($@"GET DIAGNOSTICS ""sync_row_count"" = ROW_COUNT;"); //[AB] LIMIT 1 removed to be compatible with MariaDB 10.3.x
                stringBuilder.AppendLine($"IF (sync_row_count = 0) THEN");

            }

            string empty = string.Empty;
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();
            foreach (var mutableColumn in this.tableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                var parameterName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();

                var unquotedParameterName= ParserName.Parse(mutableColumn, "\"").Unquoted().Normalized().ToString();
                var paramQuotedColumn = ParserName.Parse($"{NPGSQL_PREFIX_PARAMETER}{unquotedParameterName}", "\"").Quoted().ToString();

                stringBuilderArguments.Append(string.Concat(empty, columnName));
                stringBuilderParameters.Append(string.Concat(empty, paramQuotedColumn));
                empty = ", ";
            }

            stringBuilder.AppendLine($"\tINSERT INTO {schema}.{tableQuoted}");
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"\tSELECT * FROM ( SELECT {stringBuilderParameters.ToString()}) as TMP ");
            stringBuilder.AppendLine($"\tWHERE ( {listColumnsTmp3.ToString()} )");
            stringBuilder.AppendLine($"\tOR (ts <= sync_min_timestamp OR ts IS NULL OR t_update_scope_id = sync_scope_id OR sync_force_write = 1)");
            stringBuilder.AppendLine($"\tLIMIT 1");

            //var comma = "";
            //var strPkeys = "";
            //foreach (var column in this.tableDescription.GetPrimaryKeysColumns())
            //{
            //    strPkeys += $"{comma}{ParserName.Parse(column, "\"").Quoted().ToString()}";
            //    comma = ",";
            //}
            //stringBuilder.AppendLine($"\tON CONFLICT ({strPkeys}) DO NOTHING;");
            stringBuilder.AppendLine($"\tON CONFLICT DO NOTHING;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($@"GET DIAGNOSTICS ""sync_row_count"" = ROW_COUNT;"); //[AB] LIMIT 1 removed to be compatible with MariaDB 10.3.x
            stringBuilder.AppendLine();

            if (hasMutableColumns)
                stringBuilder.AppendLine("END IF;");

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"IF (sync_row_count > 0) THEN");
            stringBuilder.AppendLine($"\tUPDATE {schema}.{trackingTableQuoted}");
            stringBuilder.AppendLine($"\tSET \"update_scope_id\" = sync_scope_id, ");
            stringBuilder.AppendLine($"\t\t \"sync_row_is_tombstone\" = FALSE, ");
            stringBuilder.AppendLine($"\t\t \"timestamp\" = {NpgsqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine($"\t\t \"last_change_datetime\" = now() ");
            stringBuilder.AppendLine($"\tWHERE {NpgsqlManagementUtils.WhereColumnAndParameters(this.tableDescription.GetPrimaryKeysColumns(), "")};");
            stringBuilder.AppendLine($"END IF;");
            stringBuilder.AppendLine("END; ");
            stringBuilder.AppendLine("$BODY$ LANGUAGE 'plpgsql';");
            sqlCommand.Parameters.Clear();
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        
    }
}