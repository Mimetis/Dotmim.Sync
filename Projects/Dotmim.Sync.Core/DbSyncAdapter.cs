using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using Dotmim.Sync.Builders;
using System.Diagnostics;
using System.ComponentModel;
using System.Threading.Tasks;

using System.Reflection;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;

namespace Dotmim.Sync
{
    /// <summary>
    /// The SyncAdapter is the datasource manager for ONE table
    /// Should be implemented by every database provider and provide every SQL action
    /// </summary>
    public abstract class DbSyncAdapter
    {
        // Internal commands cache
        private ConcurrentDictionary<string, Lazy<SyncCommand>> commands = new ConcurrentDictionary<string, Lazy<SyncCommand>>();

        /// <summary>
        /// Gets the table description
        /// </summary>
        public SyncTable TableDescription { get; private set; }

        /// <summary>
        /// Gets the setup used 
        /// </summary>
        public SyncSetup Setup { get; }

        /// <summary>
        /// Get or Set the current step (could be only Added, Modified, Deleted)
        /// </summary>
        internal DataRowState ApplyType { get; set; }

        /// <summary>
        /// Get if the error is a primarykey exception
        /// </summary>
        public abstract bool IsPrimaryKeyViolation(Exception exception);

        /// <summary>
        /// Gets if the error is a unique key constraint exception
        /// </summary>
        public abstract bool IsUniqueKeyViolation(Exception exception);

        /// <summary>
        /// Gets a command from the current adapter
        /// </summary>
        public abstract (DbCommand Command, bool IsBatchCommand) GetCommand(DbCommandType commandType, SyncFilter filter = null);

        /// <summary>
        /// Add parameters to a command
        /// </summary>
        public abstract Task AddCommandParametersAsync(DbCommandType commandType, DbCommand command, DbConnection connection, DbTransaction transaction, SyncFilter filter = null);

        /// <summary>
        /// Execute a batch command
        /// </summary>
        public abstract Task ExecuteBatchCommandAsync(DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> arrayItems, SyncTable schemaChangesTable,
                                                      SyncTable failedRows, long? lastTimestamp, DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Create a Sync Adapter
        /// </summary>
        public DbSyncAdapter(SyncTable tableDescription, SyncSetup setup)
        {
            this.TableDescription = tableDescription;
            this.Setup = setup;
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
                        DbSyncAdapter.SetParameterValue(command, parameter.ParameterName, value);
                    }
                }

            }

            // return value
            var syncRowCountParam = DbSyncAdapter.GetParameter(command, "sync_row_count");

            if (syncRowCountParam != null)
            {
                syncRowCountParam.Direction = ParameterDirection.Output;
                syncRowCountParam.Value = DBNull.Value;
            }
        }

        /// <summary>
        /// Remove a Command from internal shared dictionary
        /// </summary>
        internal void RemoveCommands() => this.commands.Clear();

        /// <summary>
        /// Get the command from provider, check connection is opened, affect connection and transaction
        /// Prepare the command parameters and add scope parameters
        /// </summary>
        public async Task<(DbCommand Command, bool IsBatch)> GetCommandAsync(DbCommandType commandType, DbConnection connection, DbTransaction transaction, SyncFilter filter = null)
        {
            // Create the key
            var commandKey = $"{connection.DataSource}-{connection.Database}-{this.TableDescription.GetFullName()}-{commandType}";

            var (command, isBatch) = GetCommand(commandType, filter);

            if (command == null)
                return (null, false);

            // Add Parameters
            await this.AddCommandParametersAsync(commandType, command, connection, transaction, filter).ConfigureAwait(false);

            if (command == null)
                throw new MissingCommandException(commandType.ToString());

            if (connection == null)
                throw new MissingConnectionException();

            if (connection.State != ConnectionState.Open)
                throw new ConnectionClosedException(connection);

            command.Connection = connection;
            command.Transaction = transaction;

            // Get a lazy command instance
            var lazyCommand = commands.GetOrAdd(commandKey, k => new Lazy<SyncCommand>(() =>
            {
                var syncCommand = new SyncCommand(commandKey);
                return syncCommand;
            }));

            // lazyCommand.Metadata is a boolean indicating if the command is already prepared on the server
            if (lazyCommand.Value.IsPrepared == true)
                return (command, isBatch);

            // Testing The Prepare() performance increase
            command.Prepare();

            // Adding this command as prepared
            lazyCommand.Value.IsPrepared = true;

            commands.AddOrUpdate(commandKey, lazyCommand, (key, lc) => new Lazy<SyncCommand>(() => lc.Value));

            return (command, isBatch);
        }

        /// <summary>
        /// Add common parameters which could be part of the command
        /// if not found, no set done
        /// </summary>
        internal void AddScopeParametersValues(DbCommand command, Guid? id, long? lastTimestamp, bool isDeleted, bool forceWrite)
        {
            // Dotmim.Sync parameters
            DbSyncAdapter.SetParameterValue(command, "sync_force_write", forceWrite ? 1 : 0);
            DbSyncAdapter.SetParameterValue(command, "sync_min_timestamp", lastTimestamp.HasValue ? (object)lastTimestamp.Value : DBNull.Value);
            DbSyncAdapter.SetParameterValue(command, "sync_scope_id", id.HasValue ? (object)id.Value : DBNull.Value);
            DbSyncAdapter.SetParameterValue(command, "sync_row_is_tombstone", isDeleted);
        }


        // TODO : Migrate to BaseOrchestrator
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
                SyncDirection = syncTable.SyncDirection
            };

            // Adding primary keys
            foreach (var pkey in syncTable.PrimaryKeys)
                changesTable.PrimaryKeys.Add(pkey);

            // get ordered columns that are mutables and pkeys
            var orderedNames = syncTable.GetMutableColumnsWithPrimaryKeys();

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
        public static DbParameter GetParameter(DbCommand command, string parameterName)
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
        /// Set a parameter value
        /// </summary>
        public static void SetParameterValue(DbCommand command, string parameterName, object value)
        {
            var parameter = GetParameter(command, parameterName);
            if (parameter == null)
                return;

            if (value == null || value == DBNull.Value)
                parameter.Value = DBNull.Value;
            else
                parameter.Value = SyncTypeConverter.TryConvertFromDbType(value, parameter.DbType);


        }

        public static int GetSyncIntOutParameter(string parameter, DbCommand command)
        {
            DbParameter dbParameter = GetParameter(command, parameter);
            if (dbParameter == null || dbParameter.Value == null || string.IsNullOrEmpty(dbParameter.Value.ToString()))
                return 0;

            return int.Parse(dbParameter.Value.ToString(), CultureInfo.InvariantCulture);
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
