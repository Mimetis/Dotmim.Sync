using System;
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;

namespace Dotmim.Sync.Builders
{
    /// <summary>
    /// Abstract class for all database scope info  builders.
    /// </summary>
    public abstract class DbScopeBuilder
    {

        // Internal commands cache
        private ConcurrentDictionary<string, Lazy<SyncPreparedCommand>> commands = new();

        /// <summary>
        /// Gets the parsed name of the table.
        /// </summary>
        public abstract DbTableNames GetParsedScopeInfoTableNames();

        /// <summary>
        /// Gets the parsed name of the table.
        /// </summary>
        public abstract DbTableNames GetParsedScopeInfoClientTableNames();

        /// <summary>
        /// Returns a command to check if the scope_info table exists.
        /// </summary>
        public abstract DbCommand GetExistsScopeInfoTableCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to check if the scope_info_client table exists.
        /// </summary>
        public abstract DbCommand GetExistsScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to create the scope_info table.
        /// </summary>
        public abstract DbCommand GetCreateScopeInfoTableCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to create the scope_info_client table.
        /// </summary>
        public abstract DbCommand GetCreateScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to get all scope info.
        /// </summary>
        public abstract DbCommand GetAllScopeInfosCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to get all scope info clients.
        /// </summary>
        public abstract DbCommand GetAllScopeInfoClientsCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to get a scope info.
        /// </summary>
        public abstract DbCommand GetScopeInfoCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to get a scope info client.
        /// </summary>
        public abstract DbCommand GetScopeInfoClientCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to insert a new scope info.
        /// </summary>
        public abstract DbCommand GetInsertScopeInfoCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to insert a new scope info client.
        /// </summary>
        public abstract DbCommand GetInsertScopeInfoClientCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to delete a scope info.
        /// </summary>
        public abstract DbCommand GetDeleteScopeInfoCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to delete a scope info client.
        /// </summary>
        public abstract DbCommand GetDeleteScopeInfoClientCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to update a scope info.
        /// </summary>
        public abstract DbCommand GetUpdateScopeInfoCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to update a scope info client.
        /// </summary>
        public abstract DbCommand GetUpdateScopeInfoClientCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to check if a scope info exists.
        /// </summary>
        public abstract DbCommand GetLocalTimestampCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to drop the scope_info table.
        /// </summary>
        public abstract DbCommand GetDropScopeInfoTableCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to drop the scope_info_client table.
        /// </summary>
        public abstract DbCommand GetDropScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to check if a scope info exists.
        /// </summary>
        public abstract DbCommand GetExistsScopeInfoCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to check if a scope info client exists.
        /// </summary>
        public abstract DbCommand GetExistsScopeInfoClientCommand(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Remove a Command from internal shared dictionary.
        /// </summary>
        internal void RemoveCommands() => this.commands.Clear();

        /// <summary>
        /// Get the command from provider, check connection is opened, affect connection and transaction
        /// Prepare the command parameters and add scope parameters.
        /// </summary>
        internal DbCommand GetCommandAsync(DbScopeCommandType commandType, DbConnection connection, DbTransaction transaction, SyncFilter filter = null)
        {
            var scopeInfoTableNames = this.GetParsedScopeInfoTableNames();

            // Create the key
            var commandKey = $"{connection.DataSource}-{connection.Database}-{scopeInfoTableNames.NormalizedName}-{commandType}";

            var command = commandType switch
            {
                DbScopeCommandType.GetAllScopeInfos => this.GetAllScopeInfosCommand(connection, transaction),
                DbScopeCommandType.GetAllScopeInfoClients => this.GetAllScopeInfoClientsCommand(connection, transaction),

                DbScopeCommandType.GetScopeInfo => this.GetScopeInfoCommand(connection, transaction),
                DbScopeCommandType.GetScopeInfoClient => this.GetScopeInfoClientCommand(connection, transaction),

                DbScopeCommandType.CreateScopeInfoTable => this.GetCreateScopeInfoTableCommand(connection, transaction),
                DbScopeCommandType.CreateScopeInfoClientTable => this.GetCreateScopeInfoClientTableCommand(connection, transaction),

                DbScopeCommandType.ExistsScopeInfoTable => this.GetExistsScopeInfoTableCommand(connection, transaction),
                DbScopeCommandType.ExistsScopeInfoClientTable => this.GetExistsScopeInfoClientTableCommand(connection, transaction),

                DbScopeCommandType.InsertScopeInfo => this.GetInsertScopeInfoCommand(connection, transaction),
                DbScopeCommandType.InsertScopeInfoClient => this.GetInsertScopeInfoClientCommand(connection, transaction),

                DbScopeCommandType.UpdateScopeInfo => this.GetUpdateScopeInfoCommand(connection, transaction),
                DbScopeCommandType.UpdateScopeInfoClient => this.GetUpdateScopeInfoClientCommand(connection, transaction),

                DbScopeCommandType.DeleteScopeInfo => this.GetDeleteScopeInfoCommand(connection, transaction),
                DbScopeCommandType.DeleteScopeInfoClient => this.GetDeleteScopeInfoClientCommand(connection, transaction),

                DbScopeCommandType.ExistScopeInfo => this.GetExistsScopeInfoCommand(connection, transaction),
                DbScopeCommandType.ExistScopeInfoClient => this.GetExistsScopeInfoClientCommand(connection, transaction),

                DbScopeCommandType.DropScopeInfoTable => this.GetDropScopeInfoTableCommand(connection, transaction),
                DbScopeCommandType.DropScopeInfoClientTable => this.GetDropScopeInfoClientTableCommand(connection, transaction),

                DbScopeCommandType.GetLocalTimestamp => this.GetLocalTimestampCommand(connection, transaction),
                _ => throw new Exception($"This DbScopeCommandType {commandType} not exists"),
            };

            if (command == null)
                throw new MissingCommandException(commandType.ToString());

            if (connection == null)
                throw new MissingConnectionException();

            if (connection.State != ConnectionState.Open)
                throw new ConnectionClosedException(connection);

            command.Connection = connection;
            command.Transaction = transaction;

            // Get a lazy command instance
            var lazyCommand = this.commands.GetOrAdd(commandKey, k => new Lazy<SyncPreparedCommand>(() =>
            {
                var syncCommand = new SyncPreparedCommand(commandKey);
                return syncCommand;
            }));

            // lazyCommand.Metadata is a boolean indicating if the command is already prepared on the server
            if (lazyCommand.Value.IsPrepared == true)
                return command;

            // Testing The Prepare() performance increase
            command.Prepare();

            // Adding this command as prepared
            lazyCommand.Value.IsPrepared = true;

            this.commands.AddOrUpdate(commandKey, lazyCommand, (key, lc) => new Lazy<SyncPreparedCommand>(() => lc.Value));

            return command;
        }
    }
}