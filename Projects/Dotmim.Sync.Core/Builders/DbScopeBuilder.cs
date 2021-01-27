using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Builders
{
    public abstract class DbScopeBuilder
    {
        public ParserName ScopeInfoTableName { get; protected set; }

        public DbScopeBuilder(string scopeInfoTableName) => this.ScopeInfoTableName = ParserName.Parse(scopeInfoTableName);

        public abstract DbCommand GetExistsScopeInfoTableCommand(DbScopeType scopeType, DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetCreateScopeInfoTableCommand(DbScopeType scopeType, DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetAllScopesCommand(DbScopeType scopeType, DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetInsertScopeInfoCommand(DbScopeType scopeType, DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetUpdateScopeInfoCommand(DbScopeType scopeType, DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetLocalTimestampCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetDropScopeInfoTableCommand(DbScopeType scopeType, DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetExistsScopeInfoCommand(DbScopeType scopeType, DbConnection connection, DbTransaction transaction);



        // Internal commands cache
        private ConcurrentDictionary<string, Lazy<SyncCommand>> commands = new ConcurrentDictionary<string, Lazy<SyncCommand>>();

        /// <summary>
        /// Remove a Command from internal shared dictionary
        /// </summary>
        internal void RemoveCommands() => this.commands.Clear();

        /// <summary>
        /// Get the command from provider, check connection is opened, affect connection and transaction
        /// Prepare the command parameters and add scope parameters
        /// </summary>
        internal DbCommand PrepareCommand(DbScopeCommandType commandType, DbScopeType scopeType, DbConnection connection, DbTransaction transaction, SyncFilter filter = null)
        {
            // Create the key
            var commandKey = $"{connection.DataSource}-{connection.Database}-{this.ScopeInfoTableName.ToString()}-{commandType}-{scopeType}";

            // Get a lazy command instance
            var lazyCommand = commands.GetOrAdd(commandKey, k => new Lazy<SyncCommand>(() =>
            {
                var dbCommand = commandType switch
                {
                    DbScopeCommandType.ExistsScopeTable => GetExistsScopeInfoTableCommand(scopeType, connection, transaction),
                    DbScopeCommandType.CreateScopeTable => GetCreateScopeInfoTableCommand(scopeType, connection, transaction),
                    DbScopeCommandType.GetScopes => GetAllScopesCommand(scopeType, connection, transaction),
                    DbScopeCommandType.InsertScope   => GetInsertScopeInfoCommand(scopeType, connection, transaction),
                    DbScopeCommandType.UpdateScope => GetUpdateScopeInfoCommand(scopeType, connection, transaction),
                    DbScopeCommandType.ExistScope => GetExistsScopeInfoCommand(scopeType, connection, transaction),
                    DbScopeCommandType.GetLocalTimestamp => GetLocalTimestampCommand(connection, transaction),
                    DbScopeCommandType.DropScopeTable => GetDropScopeInfoTableCommand(scopeType, connection, transaction),
                    _ => throw new Exception($"This DbScopeCommandType {commandType} not exists")
                };

                var command = new SyncCommand(dbCommand);

                return command;
            }));

            // Get the concrete instance
            var command = lazyCommand.Value.DbCommand;

            if (command == null)
                throw new MissingCommandException(commandType.ToString());

            if (connection == null)
                throw new MissingConnectionException();

            if (connection.State != ConnectionState.Open)
                throw new ConnectionClosedException(connection);

            command.Connection = connection;

            if (transaction != null)
                command.Transaction = transaction;

            // lazyCommand.Metadata is a boolean indicating if the command is already prepared on the server
            if (lazyCommand.Value.IsPrepared == true)
                return command;

            // Testing The Prepare() performance increase
            command.Prepare();

            // Adding this command as prepared
            lazyCommand.Value.IsPrepared = true;

            commands.AddOrUpdate(commandKey, lazyCommand, (key, lc) => new Lazy<SyncCommand>(() => lc.Value));

            return command;
        }
    }
}
