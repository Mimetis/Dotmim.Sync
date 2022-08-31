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

        public abstract DbCommand GetExistsScopeInfoTableCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetExistsScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction);

        public abstract DbCommand GetCreateScopeInfoTableCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetCreateScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction);
         
        public abstract DbCommand GetAllScopeInfosCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetAllScopeInfoClientsCommand(DbConnection connection, DbTransaction transaction);

        public abstract DbCommand GetScopeInfoCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetScopeInfoClientCommand(DbConnection connection, DbTransaction transaction);
         
        public abstract DbCommand GetInsertScopeInfoCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetInsertScopeInfoClientCommand(DbConnection connection, DbTransaction transaction);

        public abstract DbCommand GetDeleteScopeInfoCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetDeleteScopeInfoClientCommand(DbConnection connection, DbTransaction transaction);

        public abstract DbCommand GetUpdateScopeInfoCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetUpdateScopeInfoClientCommand(DbConnection connection, DbTransaction transaction);

        public abstract DbCommand GetLocalTimestampCommand(DbConnection connection, DbTransaction transaction);

        public abstract DbCommand GetDropScopeInfoTableCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetDropScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction);

        public abstract DbCommand GetExistsScopeInfoCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetExistsScopeInfoClientCommand(DbConnection connection, DbTransaction transaction);



        // Internal commands cache
        private ConcurrentDictionary<string, Lazy<SyncPreparedCommand>> commands = new();

        /// <summary>
        /// Remove a Command from internal shared dictionary
        /// </summary>
        internal void RemoveCommands() => this.commands.Clear();

        /// <summary>
        /// Get the command from provider, check connection is opened, affect connection and transaction
        /// Prepare the command parameters and add scope parameters
        /// </summary>
        internal DbCommand GetCommandAsync(DbScopeCommandType commandType, DbConnection connection, DbTransaction transaction, SyncFilter filter = null)
        {
            // Create the key
            var commandKey = $"{connection.DataSource}-{connection.Database}-{this.ScopeInfoTableName.ToString()}-{commandType}";

            var command = commandType switch
            {
                DbScopeCommandType.GetAllScopeInfos => GetAllScopeInfosCommand(connection, transaction),
                DbScopeCommandType.GetAllScopeInfoClients => GetAllScopeInfoClientsCommand(connection, transaction),

                DbScopeCommandType.GetScopeInfo => GetScopeInfoCommand( connection, transaction),
                DbScopeCommandType.GetScopeInfoClient => GetScopeInfoClientCommand(connection, transaction),

                DbScopeCommandType.CreateScopeInfoTable => GetCreateScopeInfoTableCommand( connection, transaction),
                DbScopeCommandType.CreateScopeInfoClientTable => GetCreateScopeInfoClientTableCommand(connection, transaction),

                DbScopeCommandType.ExistsScopeInfoTable => GetExistsScopeInfoTableCommand(connection, transaction),
                DbScopeCommandType.ExistsScopeInfoClientTable => GetExistsScopeInfoClientTableCommand(connection, transaction),

                DbScopeCommandType.InsertScopeInfo => GetInsertScopeInfoCommand(connection, transaction),
                DbScopeCommandType.InsertScopeInfoClient => GetInsertScopeInfoClientCommand(connection, transaction),
                
                DbScopeCommandType.UpdateScopeInfo => GetUpdateScopeInfoCommand(connection, transaction),
                DbScopeCommandType.UpdateScopeInfoClient => GetUpdateScopeInfoClientCommand(connection, transaction),

                DbScopeCommandType.DeleteScopeInfo => GetDeleteScopeInfoCommand(connection, transaction),
                DbScopeCommandType.DeleteScopeInfoClient => GetDeleteScopeInfoClientCommand(connection, transaction),

                DbScopeCommandType.ExistScopeInfo => GetExistsScopeInfoCommand(connection, transaction),
                DbScopeCommandType.ExistScopeInfoClient => GetExistsScopeInfoClientCommand(connection, transaction),
                
                DbScopeCommandType.DropScopeInfoTable => GetDropScopeInfoTableCommand(connection, transaction),
                DbScopeCommandType.DropScopeInfoClientTable => GetDropScopeInfoClientTableCommand(connection, transaction),

                DbScopeCommandType.GetLocalTimestamp => GetLocalTimestampCommand(connection, transaction),
                _ => throw new Exception($"This DbScopeCommandType {commandType} not exists")
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
            var lazyCommand = commands.GetOrAdd(commandKey, k => new Lazy<SyncPreparedCommand>(() =>
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

            commands.AddOrUpdate(commandKey, lazyCommand, (key, lc) => new Lazy<SyncPreparedCommand>(() => lc.Value));

            return command;
        }
    }
}
