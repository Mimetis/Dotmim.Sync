
using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
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
            SyncFilter filter , DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // Create the key
            var commandKey = $"{connection.DataSource}-{connection.Database}-{syncAdapter.TableDescription.GetFullName()}-{commandType}";

            var (command, isBatch) = syncAdapter.GetCommand(commandType, filter);

            var args = new GetCommandArgs(scopeInfo, context, command, isBatch, syncAdapter.TableDescription, commandType, filter, connection, transaction);
            await this.InterceptAsync(args, progress, cancellationToken).ConfigureAwait(false);

            // Get the different command if changed by interceptor
            command = args.Command;
            isBatch = args.IsBatch;

            // IF we decide to remove the command for some reason, we silentely return 
            if (command == null)
                return (null, false);

            // Add Parameters
            if (command.Parameters == null || command.Parameters.Count == 0)
                await syncAdapter.AddCommandParametersAsync(commandType, command, connection, transaction, filter).ConfigureAwait(false);

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
        internal void SetColumnParametersValues(DbCommand command, SyncRow row)
        {
            if (row.SchemaTable == null)
                throw new ArgumentException("Schema table columns does not correspond to row values");

            var schemaTable = row.SchemaTable;

            foreach (DbParameter parameter in command.Parameters)
            {
                if (!string.IsNullOrEmpty(parameter.SourceColumn))
                {
                    // foreach parameter, check if we have a column 
                    var column = schemaTable.Columns[parameter.SourceColumn];

                    if (column != null)
                    {
                        object value = row[column] ?? DBNull.Value;
                        SetParameterValue(command, parameter.ParameterName, value);
                    }
                }

            }

            // return value
            var syncRowCountParam = InternalGetParameter(command, "sync_row_count");

            if (syncRowCountParam != null)
            {
                syncRowCountParam.Direction = ParameterDirection.Output;
                syncRowCountParam.Value = DBNull.Value;
            }
        }

        /// <summary>
        /// Add common parameters which could be part of the command
        /// if not found, no set done
        /// </summary>
        internal void AddScopeParametersValues(DbCommand command, Guid? id, long? lastTimestamp, bool isDeleted, bool forceWrite)
        {
            // Dotmim.Sync parameters
            SetParameterValue(command, "sync_force_write", forceWrite ? 1 : 0);
            SetParameterValue(command, "sync_min_timestamp", lastTimestamp.HasValue ? (object)lastTimestamp.Value : DBNull.Value);
            SetParameterValue(command, "sync_scope_id", id.HasValue ? (object)id.Value : DBNull.Value);
            SetParameterValue(command, "sync_row_is_tombstone", isDeleted);
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

            if (owner == null)
                owner = new SyncSet();

            owner.Tables.Add(changesTable);

            return changesTable;
        }

        /// <summary>
        /// Get a parameter even if it's a @param or :param or param
        /// </summary>
        internal static DbParameter InternalGetParameter(DbCommand command, string parameterName)
        {
            if (command == null)
                return null;

            if (command.Parameters.Contains($"@{parameterName}"))
                return command.Parameters[$"@{parameterName}"];

            if (command.Parameters.Contains($":{parameterName}"))
                return command.Parameters[$":{parameterName}"];

            if (command.Parameters.Contains($"in_{parameterName}"))
                return command.Parameters[$"in_{parameterName}"];

            if (!command.Parameters.Contains(parameterName))
                return null;

            return command.Parameters[parameterName];
        }

        /// <summary>
        /// Gets a parameter from a DbCommand, and set the parameter value. If neededn, can convert the value to the parameter type.
        /// <example>
        /// <para>
        /// The parameterName argument is searching for specific character, depending on the provider ("@", ":" or "in_")
        /// </para>
        /// <code>
        /// var command = connection.CreateCommand();
        /// command.CommandText = "SELECT * FROM MyTable WHERE Id = @id";
        /// command.Parameters.Add("id", DbType.Int32);
        /// SetParameterValue(command, "id", 12);
        /// </code>
        /// </example>
        /// </summary>
        public static void SetParameterValue(DbCommand command, string parameterName, object value)
        {
            var parameter = InternalGetParameter(command, parameterName);
            if (parameter == null)
                return;

            if (value == null || value == DBNull.Value)
                parameter.Value = DBNull.Value;
            else
                parameter.Value = SyncTypeConverter.TryConvertFromDbType(value, parameter.DbType);
        }

        /// <summary>
        /// Parse a time stamp value
        /// </summary>
        public static long ParseTimestamp(object obj)
        {
            if (obj == DBNull.Value)
                return 0;

            if (obj is long || obj is int || obj is ulong || obj is uint || obj is decimal)
                return Convert.ToInt64(obj, NumberFormatInfo.InvariantInfo);
            long timestamp;
            if (obj is string str)
            {
                long.TryParse(str, NumberStyles.HexNumber, CultureInfo.InvariantCulture.NumberFormat, out timestamp);
                return timestamp;
            }

            if (!(obj is byte[] numArray))
                return 0;

            var stringBuilder = new StringBuilder();
            for (int i = 0; i < numArray.Length; i++)
            {
                string str1 = numArray[i].ToString("X", NumberFormatInfo.InvariantInfo);
                stringBuilder.Append((str1.Length == 1 ? string.Concat("0", str1) : str1));
            }

            long.TryParse(stringBuilder.ToString(), NumberStyles.HexNumber, CultureInfo.InvariantCulture.NumberFormat, out timestamp);
            return timestamp;
        }
    }
}