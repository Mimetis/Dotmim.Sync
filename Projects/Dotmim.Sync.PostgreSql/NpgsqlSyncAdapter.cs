using Dotmim.Sync.Builders;
using Dotmim.Sync.PostgreSql.Builders;
using Newtonsoft.Json.Linq;
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
using static Npgsql.Replication.PgOutput.Messages.RelationMessage;

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
        private bool legacyTimestampBehavior = true;

        public NpgsqlDbMetadata NpgsqlDbMetadata { get; private set; }
        public ParserName TableName { get; }
        public ParserName TrackingTableName { get; }

        public override string QuotePrefix => "\"";
        public override string ParameterPrefix => "@";

        public NpgsqlSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName, bool useBulkOperations) : base(tableDescription, setup, scopeName, useBulkOperations)
        {
            this.NpgsqlDbMetadata = new NpgsqlDbMetadata();
            this.TableName = tableName;
            this.TrackingTableName = trackingTableName;

#if NET6_0_OR_GREATER
            // Getting EnableLegacyTimestampBehavior behavior
            this.legacyTimestampBehavior = false;
            AppContext.TryGetSwitch("Npgsql.EnableLegacyTimestampBehavior", out this.legacyTimestampBehavior);
#else
            this.legacyTimestampBehavior = true;
#endif
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

        //public Task AddCommandParametersAsync(DbCommandType commandType, DbCommand command, DbConnection connection, DbTransaction transaction = null, SyncFilter filter = null)
        //{
        //    if (command == null)
        //        return Task.CompletedTask;

        //    if (command.Parameters != null && command.Parameters.Count > 0)
        //        return Task.CompletedTask;

        //    switch (commandType)
        //    {
        //        case DbCommandType.SelectChanges:
        //        case DbCommandType.SelectChangesWithFilters:
        //        case DbCommandType.SelectInitializedChanges:
        //        case DbCommandType.SelectInitializedChangesWithFilters:
        //            this.SetSelectChangesParameters(command, filter);
        //            break;
        //        case DbCommandType.SelectRow:
        //            this.SetSelectRowParameter(command);
        //            break;
        //        case DbCommandType.DeleteMetadata:
        //            this.SetDeleteMetadataParameters(command);
        //            break;
        //        case DbCommandType.SelectMetadata:
        //            this.SetSelectMetadataParameters(command);
        //            break;
        //        case DbCommandType.DeleteRow:
        //        case DbCommandType.DeleteRows:
        //            this.SetDeleteRowParameters(command);
        //            break;
        //        case DbCommandType.UpdateRow:
        //        case DbCommandType.InsertRow:
        //        case DbCommandType.UpdateRows:
        //        case DbCommandType.InsertRows:
        //            this.AddUpdateRowParameters(command);
        //            break;
        //        case DbCommandType.UpdateMetadata:
        //            this.SetUpdateMetadataParameters(command);
        //            break;
        //        default:
        //            break;
        //    }

        //    return Task.CompletedTask;

        //}



        /// <summary>
        /// Check that parameters set from DMS core are correct.
        /// We need to add the missing parameters, and check that the existing ones are correct
        /// Uperts and Deletes commands needs the @sync_error_text output parameter
        /// </summary>
        public override DbCommand EnsureCommandParameters(DbCommand command, DbCommandType commandType, DbConnection connection, DbTransaction transaction, SyncFilter filter = null)
        {
            // For upserts & delete commands, we need to ensure that the command parameters have the additional error output parameter
            if (commandType == DbCommandType.InsertRows || commandType == DbCommandType.UpdateRows || commandType == DbCommandType.DeleteRows
                || commandType == DbCommandType.InsertRow || commandType == DbCommandType.UpdateRow || commandType == DbCommandType.DeleteRow)
            {

                string errorOutputParameterName = $"{ParameterPrefix}sync_error_text";

                var parameter = GetParameter(command, errorOutputParameterName);
                if (parameter == null)
                {
                    parameter = command.CreateParameter();
                    parameter.ParameterName = errorOutputParameterName;
                    parameter.DbType = DbType.String;
                    parameter.Direction = ParameterDirection.Output;
                    command.Parameters.Add(parameter);
                }

            }

            return command;
        }

        /// <summary>
        /// Due to new mechanisme to handle DateTime and DateTimeOffset in Postgres, we need to convert all datetime
        /// to UTC if column in database is "timestamp with time zone"
        /// </summary>
        public override DbCommand EnsureCommandParametersValues(DbCommand command, DbCommandType commandType,
            DbConnection connection, DbTransaction transaction)
        {
            foreach (NpgsqlParameter parameter in command.Parameters)
            {

                if (parameter.Value == null || parameter.Value == DBNull.Value)
                    continue;

                // Depending on framework and switch legacy, specify the kind of datetime used
                if (parameter.NpgsqlDbType == NpgsqlDbType.TimestampTz)
                {
                    if (parameter.Value is DateTime dateTime)
                        parameter.Value = legacyTimestampBehavior ? dateTime : DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);
                    else if (parameter.Value is DateTimeOffset dateTimeOffset)
                        parameter.Value = dateTimeOffset.UtcDateTime;
                    else
                    {
                        var dt = SyncTypeConverter.TryConvertTo<DateTime>(parameter.Value);
                        parameter.Value = legacyTimestampBehavior ? dt : DateTime.SpecifyKind(dt, DateTimeKind.Utc);
                    }

                }
                else if (parameter.NpgsqlDbType == NpgsqlDbType.Timestamp)
                {
                    if (parameter.Value is DateTime dateTime)
                        parameter.Value = dateTime;
                    if (parameter.Value is DateTimeOffset dateTimeOffset)
                        parameter.Value = dateTimeOffset.DateTime;
                    else
                        parameter.Value = SyncTypeConverter.TryConvertTo<DateTime>(parameter.Value);
                }
            }
            return command;
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
            stringBuilder.AppendLine($"ALTER TABLE \"{schema}\".{TableName.Quoted()} DISABLE TRIGGER ALL;");
            stringBuilder.AppendLine($"DELETE FROM \"{schema}\".{TableName.Quoted()};");
            stringBuilder.AppendLine($"DELETE FROM \"{schema}\".{TrackingTableName.Quoted()};");
            stringBuilder.AppendLine($"ALTER TABLE \"{schema}\".{TableName.Quoted()} ENABLE TRIGGER ALL;");
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
            strCommand.AppendLine($"Execute 'ALTER TABLE \"{schema}\".{TableName.Quoted()} ENABLE TRIGGER \"' || tgname || '\";';");
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
            strCommand.AppendLine($"Execute 'ALTER TABLE \"{schema}\".{TableName.Quoted()} DISABLE TRIGGER \"' || tgname || '\";';");
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

        public override Task ExecuteBatchCommandAsync(DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> arrayItems, SyncTable schemaChangesTable, SyncTable failedRows, long? lastTimestamp, DbConnection connection, DbTransaction transaction)
            => throw new NotImplementedException();
    }
}
