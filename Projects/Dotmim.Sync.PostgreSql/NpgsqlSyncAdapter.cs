﻿using Dotmim.Sync.Builders;
using Dotmim.Sync.PostgreSql.Builders;
using Newtonsoft.Json.Linq;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
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

        public NpgsqlDbMetadata NpgsqlDbMetadata { get; }
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

        public override (DbCommand, bool) GetCommand(SyncContext context, DbCommandType nameType, SyncFilter filter = null) => nameType switch
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

        /// <summary>
        /// Check that parameters set from DMS core are correct.
        /// We need to add the missing parameters, and check that the existing ones are correct
        /// Uperts and Deletes commands needs the @sync_error_text output parameter
        /// </summary>
        public override DbCommand EnsureCommandParameters(SyncContext context, DbCommand command, DbCommandType commandType, DbConnection connection, DbTransaction transaction, SyncFilter filter = null)
        {
            // For upserts & delete commands, we need to ensure that the command parameters have the additional error output parameter
            if (commandType == DbCommandType.InsertRows || commandType == DbCommandType.UpdateRows || commandType == DbCommandType.DeleteRows
                || commandType == DbCommandType.InsertRow || commandType == DbCommandType.UpdateRow || commandType == DbCommandType.DeleteRow)
            {
                string errorOutputParameterName = $"sync_error_text";
                var parameter = GetParameter(context, command, errorOutputParameterName);
                if (parameter == null)
                {
                    parameter = command.CreateParameter();
                    parameter.ParameterName = errorOutputParameterName;
                    parameter.DbType = DbType.String;
                    parameter.Direction = ParameterDirection.Output;
                    command.Parameters.Add(parameter);
                }
            }

            // Remove unecessary parameters
            if (commandType == DbCommandType.DeleteMetadata)
            {
                foreach (var column in this.TableDescription.GetPrimaryKeysColumns())
                {
                    var unquotedColumn = ParserName.Parse(column).Normalized().Unquoted().ToString();
                    var parameter = GetParameter(context, command, unquotedColumn);
                    if (parameter != null)
                        command.Parameters.Remove(parameter);
                }
            }

            if (commandType != DbCommandType.InsertRow && commandType != DbCommandType.InsertRows &&
                commandType != DbCommandType.UpdateRow && commandType != DbCommandType.UpdateRows &&
                commandType != DbCommandType.DeleteRow && commandType != DbCommandType.DeleteRows)
            {
                var parameter = GetParameter(context, command, $"sync_row_count");
                if (parameter != null)
                    command.Parameters.Remove(parameter);
            }
            return command;
        }

        /// <summary>
        /// Due to new mechanisme to handle DateTime and DateTimeOffset in Postgres, we need to convert all datetime
        /// to UTC if column in database is "timestamp with time zone"
        /// </summary>
        public override DbCommand EnsureCommandParametersValues(SyncContext context, DbCommand command, DbCommandType commandType, DbConnection connection, DbTransaction transaction)
        {
            foreach (NpgsqlParameter npgSqlParameter in command.Parameters)
            {
                if (npgSqlParameter.Value == null || npgSqlParameter.Value == DBNull.Value)
                    continue;

                AddCommandParameterValue(context, npgSqlParameter, npgSqlParameter.Value, command, commandType);
            }
            return command;
        }

        public override void AddCommandParameterValue(SyncContext context, DbParameter parameter, object value, DbCommand command, DbCommandType commandType)
        {
            var npgSqlParameter = (NpgsqlParameter)parameter;

            if (value == null || value == DBNull.Value)
            {
                npgSqlParameter.Value = DBNull.Value;
                return;
            }

            // Depending on framework and switch legacy, specify the kind of datetime used
            if (npgSqlParameter.NpgsqlDbType == NpgsqlDbType.TimestampTz)
            {
                if (value is DateTime dateTime)
                    npgSqlParameter.Value = legacyTimestampBehavior ? dateTime : DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);
                else if (value is DateTimeOffset dateTimeOffset)
                    npgSqlParameter.Value = dateTimeOffset.UtcDateTime;
                else
                {
                    var dt = SyncTypeConverter.TryConvertTo<DateTime>(value);
                    npgSqlParameter.Value = legacyTimestampBehavior ? dt : DateTime.SpecifyKind(dt, DateTimeKind.Utc);
                }
                return;
            }
            if (npgSqlParameter.NpgsqlDbType == NpgsqlDbType.Timestamp)
            {
                if (value is DateTime dateTime)
                    npgSqlParameter.Value = dateTime;
                if (value is DateTimeOffset dateTimeOffset)
                    npgSqlParameter.Value = dateTimeOffset.DateTime;
                else
                    npgSqlParameter.Value = SyncTypeConverter.TryConvertTo<DateTime>(value);
                return;
            }

            parameter.Value = SyncTypeConverter.TryConvertFromDbType(value, parameter.DbType);
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
            //var constraints = this.TableDescription.GetRelations();

            //if (constraints == null || !constraints.Any())
            //    return (null, false);
            
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(TableName);

            var strCommand = new StringBuilder();

            //strCommand.AppendLine($"DO $$");
            //strCommand.AppendLine($"BEGIN");

            //foreach (var constraint in this.TableDescription.GetRelations())
            //{
            //    var parsedTableName = ParserName.Parse(constraint.GetTable());
            //    var tableName = parsedTableName.Unquoted().ToString();
            //    var schemaName = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(parsedTableName);

            //    var parsedParentTableName = ParserName.Parse(constraint.GetParentTable());
            //    var parentTableName = parsedParentTableName.Unquoted().ToString();
            //    var parentSchemaName = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(parsedParentTableName);
            //    var relationName = constraint.RelationName;
            //    strCommand.AppendLine();
            //    strCommand.AppendLine($"IF NOT EXISTS (SELECT ct.conname FROM pg_constraint ct WHERE ct.conname = '{relationName}') THEN");
            //    strCommand.AppendLine($"ALTER TABLE \"{schemaName}\".\"{tableName}\" ");
            //    strCommand.AppendLine($"ADD CONSTRAINT \"{relationName}\" ");
            //    strCommand.Append("FOREIGN KEY (");
            //    var empty = string.Empty;
            //    foreach (var column in constraint.Keys)
            //    {
            //        var childColumnName = ParserName.Parse(column.ColumnName, "\"").Quoted().ToString();
            //        strCommand.Append($"{empty} {childColumnName}");
            //        empty = ", ";
            //    }
            //    strCommand.AppendLine(" )");
            //    strCommand.Append("REFERENCES ");
            //    strCommand.Append($"\"{parentSchemaName}\".\"{parentTableName}\"").Append(" (");
            //    empty = string.Empty;
            //    foreach (var parentdColumn in constraint.ParentKeys)
            //    {
            //        var parentColumnName = ParserName.Parse(parentdColumn.ColumnName, "\"").Quoted().ToString();
            //        strCommand.Append($"{empty} {parentColumnName}");
            //        empty = ", ";
            //    }
            //    strCommand.AppendLine(" ) NOT VALID; "); // NOT VALID is important to avoid re scan of the table afterwards
            //    strCommand.AppendLine("END IF;");

            //}
            //strCommand.AppendLine();
            //strCommand.AppendLine($"END $$;");

            strCommand.AppendLine($"ALTER TABLE \"{schema}\".{TableName.Quoted()}  ENABLE TRIGGER ALL;");

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

            //var constraints = this.TableDescription.GetRelations();

            //if (constraints == null || !constraints.Any())
            //    return (null, false);


            var strCommand = new StringBuilder();
            //strCommand.AppendLine($"DO $$");
            //strCommand.AppendLine($"Declare fkname character varying(250);");
            //strCommand.AppendLine($"Declare cur_trg cursor For ");
            //strCommand.AppendLine($"Select ct.conname");
            //strCommand.AppendLine($"From pg_constraint ct ");
            //strCommand.AppendLine($"join pg_catalog.pg_class  cl on  cl.oid =  ct.conrelid");
            //strCommand.AppendLine($"join pg_catalog.pg_namespace nsp on nsp.oid = ct.connamespace");
            //strCommand.AppendLine($"where relname = '{TableName.Unquoted()}' and ct.contype = 'f' and nspname='{schema}';");
            //strCommand.AppendLine($"BEGIN");
            //strCommand.AppendLine($"open cur_trg;");
            //strCommand.AppendLine($"loop");
            //strCommand.AppendLine($"fetch cur_trg into fkname;");
            //strCommand.AppendLine($"exit when not found;");
            //strCommand.AppendLine($"Execute 'ALTER TABLE \"{schema}\".{TableName.Quoted()} DROP CONSTRAINT \"' || fkname || '\";';");
            //strCommand.AppendLine($"end loop;");
            //strCommand.AppendLine($"close cur_trg;");
            //strCommand.AppendLine($"END $$;");

            strCommand.AppendLine($"ALTER TABLE \"{schema}\".{TableName.Quoted()}  DISABLE TRIGGER ALL;");

            var command = new NpgsqlCommand
            {
                CommandType = CommandType.Text,
                CommandText = strCommand.ToString()
            };
            return (command, false);
        }

        public override Task ExecuteBatchCommandAsync(SyncContext context, DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> arrayItems, SyncTable schemaChangesTable, SyncTable failedRows, long? lastTimestamp, DbConnection connection, DbTransaction transaction)
            => throw new NotImplementedException();
    }
}
