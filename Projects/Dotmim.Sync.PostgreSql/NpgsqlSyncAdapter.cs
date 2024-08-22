using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;
using Dotmim.Sync.PostgreSql.Builders;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.PostgreSql
{
    /// <summary>
    /// Npgsql Sync Adapter.
    /// </summary>
    public partial class NpgsqlSyncAdapter : DbSyncAdapter
    {
        // private bool legacyTimestampBehavior = true;

        /// <summary>
        /// Returns the timestamp value for PostgreSql.
        /// </summary>
        public const string TimestampValue = "(extract(epoch from now())*1000)";

        /// <summary>
        /// Gets the ,npgsql object names.
        /// </summary>
        public NpgsqlObjectNames NpgsqlObjectNames { get; }

        /// <summary>
        /// Gets the npgsql database metadata.
        /// </summary>
        public NpgsqlDbMetadata NpgsqlDbMetadata { get; }

        /// <inheritdoc />
        public override string ParameterPrefix => "@";

        /// <inheritdoc cref="NpgsqlSyncAdapter"/>
        public NpgsqlSyncAdapter(SyncTable tableDescription, ScopeInfo scopeInfo, bool useBulkOperations)
            : base(tableDescription, scopeInfo, useBulkOperations)
        {
            this.NpgsqlDbMetadata = new NpgsqlDbMetadata();
            this.NpgsqlObjectNames = new NpgsqlObjectNames(tableDescription, scopeInfo);

            // #if NET6_0_OR_GREATER
            //            // Getting EnableLegacyTimestampBehavior behavior
            //            this.legacyTimestampBehavior = false;
            //            AppContext.TryGetSwitch("Npgsql.EnableLegacyTimestampBehavior", out this.legacyTimestampBehavior);
            // #else
            //            this.legacyTimestampBehavior = true;
            // #endif
        }

        /// <inheritdoc/>
        public override DbColumnNames GetParsedColumnNames(string name)
        {
            var columnParser = new ObjectParser(name, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
            return new DbColumnNames(columnParser.QuotedShortName, columnParser.NormalizedShortName);
        }

        /// <inheritdoc/>
        public override DbTableBuilder GetTableBuilder() => new NpgsqlTableBuilder(this.TableDescription, this.ScopeInfo);

        /// <inheritdoc />
        public override (DbCommand, bool) GetCommand(SyncContext context, DbCommandType commandType, SyncFilter filter = null) => commandType switch
        {
            DbCommandType.SelectChanges => this.GetSelectChangesCommand(),
            DbCommandType.SelectInitializedChanges => this.GetSelectInitializedChangesCommand(),
            DbCommandType.SelectInitializedChangesWithFilters => this.GetSelectInitializedChangesCommand(filter),
            DbCommandType.SelectChangesWithFilters => this.GetSelectChangesCommand(filter),
            DbCommandType.SelectRow => this.GetSelectRowCommand(),
            DbCommandType.UpdateRow or DbCommandType.InsertRow or DbCommandType.UpdateRows or DbCommandType.InsertRows
            => this.GetUpdateRowCommand(),
            DbCommandType.DeleteRow or DbCommandType.DeleteRows => this.GetDeleteRowCommand(),
            DbCommandType.DisableConstraints => this.GetDisableConstraintCommand(),
            DbCommandType.EnableConstraints => this.GetEnableConstraintCommand(),
            DbCommandType.DeleteMetadata => this.GetDeleteMetadataCommand(),
            DbCommandType.UpdateMetadata => this.CreateUpdateMetadataCommand(),
            DbCommandType.SelectMetadata => this.CreateSelectMetadataCommand(),
            DbCommandType.UpdateUntrackedRows => this.CreateUpdateUntrackedRowsCommand(),
            DbCommandType.Reset => this.GetResetCommand(),
            DbCommandType.PreDeleteRow or DbCommandType.PreDeleteRows => this.CreatePreDeleteCommand(),
            DbCommandType.PreInsertRow or DbCommandType.PreInsertRows or DbCommandType.PreUpdateRow or DbCommandType.PreUpdateRows => this.CreatePreUpdateCommand(),
            _ => throw new NotImplementedException($"This command type {commandType} is not implemented"),
        };

        /// <summary>
        /// Check that parameters set from DMS core are correct.
        /// We need to add the missing parameters, and check that the existing ones are correct
        /// Uperts and Deletes commands needs the @sync_error_text output parameter.
        /// </summary>
        public override DbCommand EnsureCommandParameters(SyncContext context, DbCommand command, DbCommandType commandType, DbConnection connection, DbTransaction transaction, SyncFilter filter = null)
        {
            // For upserts & delete commands, we need to ensure that the command parameters have the additional error output parameter
            if (commandType == DbCommandType.InsertRows || commandType == DbCommandType.UpdateRows || commandType == DbCommandType.DeleteRows
                || commandType == DbCommandType.InsertRow || commandType == DbCommandType.UpdateRow || commandType == DbCommandType.DeleteRow)
            {
                string errorOutputParameterName = $"sync_error_text";
                var parameter = this.GetParameter(context, command, errorOutputParameterName);
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
                    var columnParser = new ObjectParser(column.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                    var parameter = this.GetParameter(context, command, columnParser.ObjectName);
                    if (parameter != null)
                        command.Parameters.Remove(parameter);
                }
            }

            if (commandType != DbCommandType.InsertRow && commandType != DbCommandType.InsertRows &&
                commandType != DbCommandType.UpdateRow && commandType != DbCommandType.UpdateRows &&
                commandType != DbCommandType.DeleteRow && commandType != DbCommandType.DeleteRows)
            {
                var parameter = this.GetParameter(context, command, $"sync_row_count");
                if (parameter != null)
                    command.Parameters.Remove(parameter);
            }

            return command;
        }

        /// <summary>
        /// Due to new mechanisme to handle DateTime and DateTimeOffset in Postgres, we need to convert all datetime
        /// to UTC if column in database is "timestamp with time zone".
        /// </summary>
        public override DbCommand EnsureCommandParametersValues(SyncContext context, DbCommand command, DbCommandType commandType, DbConnection connection, DbTransaction transaction)
        {
            foreach (NpgsqlParameter npgSqlParameter in command.Parameters)
            {
                if (npgSqlParameter.Value == null || npgSqlParameter.Value == DBNull.Value)
                    continue;

                this.AddCommandParameterValue(context, npgSqlParameter, npgSqlParameter.Value, command, commandType);
            }

            return command;
        }

        /// <inheritdoc />
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
                npgSqlParameter.Value = value switch
                {
                    DateTime dateTime => DateTime.SpecifyKind(dateTime, DateTimeKind.Utc),
                    DateTimeOffset dateTimeOffset => dateTimeOffset.UtcDateTime,
                    _ => DateTime.SpecifyKind(SyncTypeConverter.TryConvertTo<DateTime>(value), DateTimeKind.Utc),
                };
                return;
            }

            if (npgSqlParameter.NpgsqlDbType == NpgsqlDbType.Timestamp)
            {
                npgSqlParameter.Value = value switch
                {
                    DateTime dateTime => dateTime,
                    DateTimeOffset dateTimeOffset => dateTimeOffset.DateTime,
                    _ => SyncTypeConverter.TryConvertTo<DateTime>(value),
                };
                return;
            }

            parameter.Value = SyncTypeConverter.TryConvertFromDbType(value, parameter.DbType);
        }

        // ---------------------------------------------------
        // Reset Command
        // ---------------------------------------------------

        /// <summary>
        /// Returns a command to reset a table.
        /// </summary>
        private (DbCommand Command, bool IsBatchCommand) GetResetCommand()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"DO $$");
            stringBuilder.AppendLine($"");
            stringBuilder.AppendLine("BEGIN");
            stringBuilder.AppendLine($"ALTER TABLE {this.NpgsqlObjectNames.TableQuotedFullName} DISABLE TRIGGER ALL;");
            stringBuilder.AppendLine($"DELETE FROM {this.NpgsqlObjectNames.TableQuotedFullName};");
            stringBuilder.AppendLine($"DELETE FROM {this.NpgsqlObjectNames.TrackingTableQuotedFullName};");
            stringBuilder.AppendLine($"ALTER TABLE {this.NpgsqlObjectNames.TrackingTableQuotedFullName} ENABLE TRIGGER ALL;");
            stringBuilder.AppendLine("END $$;");

            var command = new NpgsqlCommand
            {
                CommandType = CommandType.Text,
                CommandText = stringBuilder.ToString(),
            };
            return (command, false);
        }

        // ---------------------------------------------------
        // Enable Constraints Command
        // ---------------------------------------------------
        private (DbCommand Command, bool IsBatchCommand) GetEnableConstraintCommand()
        {
            var command = new NpgsqlCommand
            {
                CommandType = CommandType.Text,
                CommandText = $"ALTER TABLE {this.NpgsqlObjectNames.TableQuotedFullName} ENABLE TRIGGER ALL;",
            };

            return (command, false);
        }

        // ---------------------------------------------------
        // Disable Constraints Command
        // ---------------------------------------------------
        private (DbCommand Command, bool IsBatchCommand) GetDisableConstraintCommand()
        {
            var command = new NpgsqlCommand
            {
                CommandType = CommandType.Text,
                CommandText = $"ALTER TABLE {this.NpgsqlObjectNames.TableQuotedFullName}  DISABLE TRIGGER ALL;",
            };
            return (command, false);
        }

        /// <summary>
        /// Not implemented for PostgreSql.
        /// </summary>
        public override Task ExecuteBatchCommandAsync(SyncContext context, DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> arrayItems, SyncTable schemaChangesTable, SyncTable failedRows, long? lastTimestamp, DbConnection connection, DbTransaction transaction)
            => throw new NotImplementedException();
    }
}