using Dotmim.Sync.Builders;
using Dotmim.Sync.PostgreSql.Builders;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;

namespace Dotmim.Sync.PostgreSql
{
    public partial class NpgsqlSyncAdapter : DbSyncAdapter
    {
        public const string TimestampValue = "(extract(epoch from now())*1000)";
        internal const string insertTriggerName = "{0}insert_trigger";
        internal const string updateTriggerName = "{0}update_trigger";
        internal const string deleteTriggerName = "{0}delete_trigger";

        internal const string selectChangesProcName = @"{0}.""{1}{2}changes""";
        internal const string selectChangesProcNameWithFilters = @"{0}.""{1}{2}{3}changes""";

        internal const string initializeChangesProcName = @"{0}.""{1}{2}initialize""";
        internal const string initializeChangesProcNameWithFilters = @"{0}.""{1}{2}{3}initialize""";

        internal const string selectRowProcName = @"{0}.""{1}{2}selectrow""";

        internal const string insertProcName = @"{0}.""{1}{2}insert""";
        internal const string updateProcName = @"{0}.""{1}{2}update""";
        internal const string deleteProcName = @"{0}.""{1}{2}delete""";

        internal const string deleteMetadataProcName = @"{0}.""{1}{2}deletemetadata""";

        internal const string resetMetadataProcName = @"{0}.""{1}{2}reset""";

        public NpgsqlDbMetadata NpgsqlDbMetadata { get; private set; }
        public ParserName TableName { get; }
        public ParserName TrackingTableName { get; }

        public NpgsqlSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName, bool useBulkOperations) : base(tableDescription, setup, scopeName, useBulkOperations)
        {
            this.NpgsqlDbMetadata = new NpgsqlDbMetadata();
            this.TableName = tableName;
            this.TrackingTableName = trackingTableName;
        }

        public override (DbCommand, bool) GetCommand(DbCommandType nameType, SyncFilter filter = null) => nameType switch
        {
            DbCommandType.SelectChanges => GetSelectChangesCommand(),
            DbCommandType.SelectInitializedChanges => GetSelectInitializedChangesCommand(),
            DbCommandType.SelectInitializedChangesWithFilters => GetSelectInitializedChangesCommand(filter),
            DbCommandType.SelectChangesWithFilters => GetSelectChangesCommand(filter),
            DbCommandType.SelectRow => GetSelectRowCommand(),
            DbCommandType.UpdateRow or DbCommandType.InsertRow or DbCommandType.UpdateRows or DbCommandType.InsertRows => GetUpdateRowCommand(),
            DbCommandType.DeleteRow or DbCommandType.DeleteRows => GetDeleteRowCommand(),
            DbCommandType.DisableConstraints => GetDisableConstraintCommand(),
            DbCommandType.EnableConstraints => GetEnableConstraintCommand(),
            DbCommandType.DeleteMetadata => GetDeleteMetadataCommand(),
            DbCommandType.UpdateMetadata => CreateUpdateMetadataCommand(),
            DbCommandType.SelectMetadata => CreateSelectMetadataCommand(),
            DbCommandType.UpdateUntrackedRows => CreateUpdateUntrackedRowsCommand(),
            DbCommandType.Reset => GetResetCommand(),
            DbCommandType.PreDeleteRow or DbCommandType.PreDeleteRows => CreatePreDeleteCommand(),
            DbCommandType.PreInsertRow or DbCommandType.PreInsertRows or DbCommandType.PreUpdateRow or DbCommandType.PreUpdateRows => CreatePreUpdateCommand(),
            _ => throw new NotImplementedException($"This command type {nameType} is not implemented"),
        };

        public override Task AddCommandParametersAsync(DbCommandType commandType, DbCommand command, DbConnection connection, DbTransaction transaction = null, SyncFilter filter = null)
        {
            if (command == null)
                return Task.CompletedTask;

            if (command.Parameters != null && command.Parameters.Count > 0)
                return Task.CompletedTask;

            switch (commandType)
            {
                case DbCommandType.SelectChanges:
                case DbCommandType.SelectChangesWithFilters:
                case DbCommandType.SelectInitializedChanges:
                case DbCommandType.SelectInitializedChangesWithFilters:
                    this.SetSelectChangesParameters(command, filter);
                    break;
                case DbCommandType.SelectRow:
                    this.SetSelectRowParameter(command);
                    break;
                case DbCommandType.DeleteMetadata:
                    this.SetDeleteMetadataParameters(command);
                    break;
                case DbCommandType.SelectMetadata:
                    this.SetSelectMetadataParameters(command);
                    break;
                case DbCommandType.DeleteRow:
                case DbCommandType.DeleteRows:
                    this.SetDeleteRowParameters(command);
                    break;
                case DbCommandType.UpdateRow:
                case DbCommandType.InsertRow:
                case DbCommandType.UpdateRows:
                case DbCommandType.InsertRows:
                    this.SetUpdateRowParameters(command);
                    break;
                case DbCommandType.UpdateMetadata:
                    this.SetUpdateMetadataParameters(command);
                    break;
                default:
                    break;
            }

            return Task.CompletedTask;
        }

        // ---------------------------------------------------
        // Reset Command
        // ---------------------------------------------------

        private (DbCommand, bool) GetResetCommand()
        {
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(TableName);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"DO $$");
            stringBuilder.AppendLine($"");
            stringBuilder.AppendLine("BEGIN");
            stringBuilder.AppendLine($"ALTER TABLE {schema}.{TableName.Quoted()} DISABLE TRIGGER ALL;");
            stringBuilder.AppendLine($"DELETE FROM {schema}.{TableName.Quoted()};");
            stringBuilder.AppendLine($"DELETE FROM {schema}.{TrackingTableName.Quoted()};");
            stringBuilder.AppendLine($"ALTER TABLE {schema}.{TableName.Quoted()} ENABLE TRIGGER ALL;");
            stringBuilder.AppendLine("END $$;");

            var command = new NpgsqlCommand
            {
                CommandType = CommandType.Text,
                CommandText = stringBuilder.ToString()
            };
            return (command, false);
        }

        // ---------------------------------------------------
        // Enable Constraints Command
        // ---------------------------------------------------

        private (DbCommand, bool) GetEnableConstraintCommand()
        {
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(TableName);

            var strCommand = new StringBuilder();
            strCommand.AppendLine($"DO $$");
            strCommand.AppendLine($"Declare tgname character varying(250);");
            strCommand.AppendLine($"Declare cur_trg cursor For ");
            strCommand.AppendLine($"Select tr.tgname");
            strCommand.AppendLine($"from pg_catalog.pg_trigger tr ");
            strCommand.AppendLine($"join pg_catalog.pg_class  cl on  cl.oid =  tr.tgrelid");
            strCommand.AppendLine($"join pg_constraint ct on ct.oid = tr.tgconstraint");
            strCommand.AppendLine($"join pg_catalog.pg_namespace nsp on nsp.oid = ct.connamespace");
            strCommand.AppendLine($"where relname = '{TableName.Unquoted()}' and tgconstraint != 0 and nspname='{schema}';");
            strCommand.AppendLine($"BEGIN");
            strCommand.AppendLine($"open cur_trg;");
            strCommand.AppendLine($"loop");
            strCommand.AppendLine($"fetch cur_trg into tgname;");
            strCommand.AppendLine($"exit when not found;");
            strCommand.AppendLine($"Execute 'ALTER TABLE {schema}.{TableName.Quoted()} ENABLE TRIGGER \"' || tgname || '\";';");
            strCommand.AppendLine($"end loop;");
            strCommand.AppendLine($"close cur_trg;");
            strCommand.AppendLine($"END $$;");

            var command = new NpgsqlCommand
            {
                CommandType = CommandType.Text,
                CommandText = strCommand.ToString()
            };
            return (command, false);
        }

        // ---------------------------------------------------
        // Disable Constraints Command
        // ---------------------------------------------------

        private (DbCommand, bool) GetDisableConstraintCommand()
        {
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(TableName);

            var strCommand = new StringBuilder();
            strCommand.AppendLine($"DO $$");
            strCommand.AppendLine($"Declare tgname character varying(250);");
            strCommand.AppendLine($"Declare cur_trg cursor For ");
            strCommand.AppendLine($"Select tr.tgname");
            strCommand.AppendLine($"from pg_catalog.pg_trigger tr ");
            strCommand.AppendLine($"join pg_catalog.pg_class  cl on  cl.oid =  tr.tgrelid");
            strCommand.AppendLine($"join pg_constraint ct on ct.oid = tr.tgconstraint");
            strCommand.AppendLine($"join pg_catalog.pg_namespace nsp on nsp.oid = ct.connamespace");
            strCommand.AppendLine($"where relname = '{TableName.Unquoted()}' and tgconstraint != 0 and nspname='{schema}';");
            strCommand.AppendLine($"BEGIN");
            strCommand.AppendLine($"open cur_trg;");
            strCommand.AppendLine($"loop");
            strCommand.AppendLine($"fetch cur_trg into tgname;");
            strCommand.AppendLine($"exit when not found;");
            strCommand.AppendLine($"Execute 'ALTER TABLE {schema}.{TableName.Quoted()} DISABLE TRIGGER \"' || tgname || '\";';");
            strCommand.AppendLine($"end loop;");
            strCommand.AppendLine($"close cur_trg;");
            strCommand.AppendLine($"END $$;");

            var command = new NpgsqlCommand
            {
                CommandType = CommandType.Text,
                CommandText = strCommand.ToString()
            };
            return (command, false);
        }

        // ---------------------------------------------------

        private NpgsqlParameter GetSqlParameter(SyncColumn column, string prefix)
        {
            var paramName = $"{prefix}{ParserName.Parse(column).Unquoted().Normalized()}";
            var paramNameQuoted = ParserName.Parse(paramName, "\"").Quoted().ToString();
            var sqlParameter = new NpgsqlParameter
            {
                ParameterName = paramNameQuoted
            };

            // Get the good SqlDbType (even if we are not from Sql Server def)
            var sqlDbType = this.TableDescription.OriginalProvider == NpgsqlSyncProvider.ProviderType ?
                this.NpgsqlDbMetadata.GetNpgsqlDbType(column) : this.NpgsqlDbMetadata.GetOwnerDbTypeFromDbType(column);


            sqlParameter.NpgsqlDbType = sqlDbType;
            sqlParameter.IsNullable = column.AllowDBNull;

            var (p, s) = this.NpgsqlDbMetadata.GetCompatibleColumnPrecisionAndScale(column, this.TableDescription.OriginalProvider);

            if (p > 0)
            {
                sqlParameter.Precision = p;
                if (s > 0)
                    sqlParameter.Scale = s;
            }

            var m = this.NpgsqlDbMetadata.GetCompatibleMaxLength(column, this.TableDescription.OriginalProvider);

            if (m > 0)
                sqlParameter.Size = m;

            return sqlParameter;
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

            var columnDeclarationString = this.NpgsqlDbMetadata.GetCompatibleColumnTypeDeclarationString(tmpColumn, this.TableDescription.OriginalProvider);


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


        public override Task ExecuteBatchCommandAsync(DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> arrayItems, SyncTable schemaChangesTable, SyncTable failedRows, long? lastTimestamp, DbConnection connection, DbTransaction transaction)
            => throw new NotImplementedException();
    }
}
