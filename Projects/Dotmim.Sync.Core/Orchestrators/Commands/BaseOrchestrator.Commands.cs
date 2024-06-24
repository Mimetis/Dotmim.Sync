
using Dotmim.Sync.Builders;
using System;
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class BaseOrchestrator
    {

        private static ConcurrentDictionary<string, Lazy<SyncPreparedCommand>> PreparedCommands = new();

        /// <summary>
        /// Get the command from provider, check connection is opened, affect connection and transaction
        /// Prepare the command parameters and add scope parameters
        /// </summary>
        internal async Task<(DbCommand Command, bool IsBatch)> InternalGetCommandAsync(ScopeInfo scopeInfo, SyncContext context, DbSyncAdapter syncAdapter, DbCommandType commandType,
            DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            SyncFilter filter = null;

            //if (this.Provider != null && this.Provider.CanBeServerProvider) // Sqlite can't be server
            if (this.Provider != null) // trying for Sqlite too
                filter = syncAdapter.TableDescription.GetFilter();

            var (command, isBatch) = syncAdapter.GetCommand(context, commandType, filter);

            // IF we do not have any command associated, just return
            if (command == null)
                return (null, false);

            // Command Timeout if set in Options
            if (this.Options.DbCommandTimeout.HasValue)
                command.CommandTimeout = Options.DbCommandTimeout.Value;

            // Create the key
            var commandKey = $"{connection.DataSource}-{connection.Database}-{syncAdapter.TableDescription.GetFullName()}-{commandType}";

            // Add Parameters
            if (command.Parameters == null || command.Parameters.Count == 0)
            {
                command = commandType switch
                {
                    DbCommandType.None => command,
                    DbCommandType.SelectChanges or DbCommandType.SelectChangesWithFilters => InternalSetSelectChangesParameters(command, syncAdapter),
                    DbCommandType.SelectInitializedChanges or DbCommandType.SelectInitializedChangesWithFilters => InternalSetSelectInitializeChangesParameters(command, syncAdapter),
                    DbCommandType.SelectRow => InternalSetSelectRowParameters(command, syncAdapter),
                    DbCommandType.UpdateRow or DbCommandType.UpdateRows or DbCommandType.InsertRow or DbCommandType.InsertRows => InternalSetUpsertsParameters(command, syncAdapter),
                    DbCommandType.DeleteRow or DbCommandType.DeleteRows => InternalSetDeleteRowParameters(command, syncAdapter),
                    DbCommandType.DeleteMetadata => InternalSetDeleteMetadataParameters(command, syncAdapter),
                    DbCommandType.UpdateMetadata => InternalSetUpdateMetadataParameters(command, syncAdapter),
                    DbCommandType.SelectMetadata => InternalSetSelectMetadataParameters(command, syncAdapter),
                    DbCommandType.Reset => InternalSetResetParameters(command, syncAdapter),
                    _ => command,
                };

            }

            // Ensure parameters are correct, from DbSyncAdapter
            command = syncAdapter.EnsureCommandParameters(context, command, commandType, connection, transaction);

            // Let a chance to the interceptor to change the command
            var args = new GetCommandArgs(scopeInfo, context, command, isBatch, syncAdapter.TableDescription, commandType, connection, transaction);
            await this.InterceptAsync(args, progress, cancellationToken).ConfigureAwait(false);

            // Get the different command if changed by interceptor
            command = args.Command;
            isBatch = args.IsBatch;

            // IF we decide to remove the command for some reason, we silentely return 
            if (command == null)
                return (null, false);

            // From that point if command is null, it's an error raised
            if (command == null)
                throw new MissingCommandException(commandType.ToString());

            if (connection == null)
                throw new MissingConnectionException();

            if (connection.State != ConnectionState.Open)
                throw new ConnectionClosedException(connection);

            command.Connection = connection;
            command.Transaction = transaction;

            // Check the command has been already prepared
            // Get a lazy command instance
            // Try to get the instance
            var lazyCommand = PreparedCommands.GetOrAdd(commandKey, k =>
                new Lazy<SyncPreparedCommand>(() => new SyncPreparedCommand(commandKey)));

            // lazyCommand.Metadata is a boolean indicating if the command is already prepared on the server
            if (lazyCommand.Value.IsPrepared == true)
                return (command, isBatch);

            // Testing The Prepare() performance increase
            command.Prepare();

            // Adding this command as prepared
            lazyCommand.Value.IsPrepared = true;

            PreparedCommands.AddOrUpdate(commandKey, lazyCommand, (key, lc) => new Lazy<SyncPreparedCommand>(() => lc.Value));

            return (command, isBatch);
        }


        /// <summary>
        /// Set command parameters value mapped to Row
        /// </summary>
        internal DbCommand InternalSetCommandParametersValues(SyncContext context, DbCommand command, DbCommandType commandType, DbSyncAdapter syncAdapter,
            DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress,
            SyncRow row = null, Guid? sync_scope_id = null, long? sync_min_timestamp = null, bool? sync_row_is_tombstone = null, bool? sync_force_write = null)
        {
            if (row != null && row.SchemaTable == null)
                throw new ArgumentException("Schema table columns does not correspond to row values");

            var schemaTable = syncAdapter.TableDescription;

            // Check filters
            SyncFilter tableFilter = null;

            if (this.Provider != null)
                tableFilter = schemaTable.GetFilter();

            // if row is not null, fill the parameters
            if (row != null)
            {
                for (var i = 0; i < command.Parameters.Count; i++)
                {
                    var parameter = command.Parameters[i];

                    // Check if it's a parameter from the schema table
                    if (!string.IsNullOrEmpty(parameter.SourceColumn))
                    {
                        // foreach parameter, check if we have a column 
                        var columnIndex = schemaTable.Columns.IndexOf(parameter.SourceColumn);

                        if (columnIndex >= 0)
                        {
                            object value = row[columnIndex] ?? DBNull.Value;
                            syncAdapter.AddCommandParameterValue(context, parameter, value, command, commandType);
                        }
                    }

                }
            }

            // if we have a filter, set the filter parameters
            if (tableFilter != null && tableFilter.Parameters != null && tableFilter.Parameters.Count > 0)
            {
                // context parameters can be null at some point.
                var contexParameters = context.Parameters ?? new SyncParameters();

                foreach (var filterParam in tableFilter.Parameters)
                {
                    var param = syncAdapter.GetParameter(context, command, filterParam.Name);

                    if (param != null)
                    {
                        var parameter = contexParameters.FirstOrDefault(p => p.Name.Equals(filterParam.Name, SyncGlobalization.DataSourceStringComparison));
                        syncAdapter.AddCommandParameterValue(context, param, parameter?.Value, command, commandType);
                    }
                }
            }

            // Common parameters that could be in the command

            // Set the scope id
            var syncScopeIdParameter = syncAdapter.GetParameter(context, command, "sync_scope_id");
            if (syncScopeIdParameter != null)
                syncAdapter.AddCommandParameterValue(context, syncScopeIdParameter, sync_scope_id.HasValue ? sync_scope_id.Value : DBNull.Value, command, commandType);

            // Set the sync_min_timestamp
            var syncMinTimestampParameter = syncAdapter.GetParameter(context, command, "sync_min_timestamp");
            if (syncMinTimestampParameter != null)
                syncAdapter.AddCommandParameterValue(context, syncMinTimestampParameter, sync_min_timestamp.HasValue ? sync_min_timestamp.Value : DBNull.Value, command, commandType);

            // Set the sync_row_timestamp
            // glitch in delete metadata command 
            var syncSyncRowTimestamp = syncAdapter.GetParameter(context, command, "sync_row_timestamp");
            if (syncSyncRowTimestamp != null)
                syncAdapter.AddCommandParameterValue(context, syncSyncRowTimestamp, sync_min_timestamp.HasValue ? sync_min_timestamp.Value : DBNull.Value, command, commandType);

            // Set the sync_row_is_tombstone
            var syncRowIsTombstoneParameter = syncAdapter.GetParameter(context, command, "sync_row_is_tombstone");
            if (syncRowIsTombstoneParameter != null)
                syncAdapter.AddCommandParameterValue(context, syncRowIsTombstoneParameter, sync_row_is_tombstone.HasValue ? sync_row_is_tombstone.Value ? 1 : 0 : DBNull.Value, command, commandType);

            // Set the sync_force_write
            var syncForceWriteParameter = syncAdapter.GetParameter(context, command, "sync_force_write");
            if (syncForceWriteParameter != null)
                syncAdapter.AddCommandParameterValue(context, syncForceWriteParameter, sync_force_write.HasValue ? sync_force_write.Value ? 1 : 0 : DBNull.Value, command, commandType);

            // Sqlite does not support output parameters
            if (syncAdapter.SupportsOutputParameters)
            {
                // return value
                var syncRowCountParam = syncAdapter.GetParameter(context, command, "sync_row_count");
                if (syncRowCountParam != null)
                {
                    syncRowCountParam.Direction = ParameterDirection.Output;
                    syncAdapter.AddCommandParameterValue(context, syncRowCountParam, DBNull.Value, command, commandType);
                }

                // error text
                var syncErrorTextParam = syncAdapter.GetParameter(context, command, "sync_error_text");
                if (syncErrorTextParam != null)
                {
                    syncErrorTextParam.Direction = ParameterDirection.Output;
                    syncAdapter.AddCommandParameterValue(context, syncErrorTextParam, DBNull.Value, command, commandType);
                }
            }

            // Ensure parameters are correct, from DbSyncAdapter
            command = syncAdapter.EnsureCommandParametersValues(context, command, commandType, connection, transaction);

            return command;
        }

        /// <summary>
        /// Remove a Command from internal shared dictionary
        /// </summary>
        internal void RemoveCommands() => PreparedCommands.Clear();

        /// <summary>
        /// Create a change table with scope columns and tombstone column
        /// </summary>
        public static SyncTable CreateChangesTable(SyncTable syncTable, SyncSet owner = null)
        {
            if (syncTable.Schema == null)
                throw new ArgumentException("Schema can't be null when creating a changes table");

            // Create an empty sync table without columns
            var changesTable = new SyncTable(syncTable.TableName, syncTable.SchemaName)
            {
                OriginalProvider = syncTable.OriginalProvider,
                //SyncDirection = syncTable.SyncDirection
            };

            // Adding primary keys
            foreach (var pkey in syncTable.PrimaryKeys)
                changesTable.PrimaryKeys.Add(pkey);

            // get ordered columns that are mutables and pkeys
            var orderedNames = syncTable.GetMutableColumns(false, true);

            foreach (var c in orderedNames)
                changesTable.Columns.Add(c.Clone());

            if (owner != null)
            {
                owner.Tables.Add(changesTable);
            }

            return changesTable;
        }


    }
}