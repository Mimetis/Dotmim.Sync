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

        public abstract DbCommand GetExistsClientScopeInfoTableCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetExistsServerScopeInfoTableCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetExistsServerHistoryScopeInfoTableCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetCreateClientScopeInfoTableCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetCreateServerScopeInfoTableCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetCreateServerHistoryScopeInfoTableCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetAllClientScopesInfoCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetAllServerScopesInfoCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetAllServerHistoriesScopesInfoCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetClientScopeInfoCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetServerScopeInfoCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetServerHistoryScopeInfoCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetInsertClientScopeInfoCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetInsertServerScopeInfoCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetInsertServerHistoryScopeInfoCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetDeleteClientScopeInfoCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetDeleteServerScopeInfoCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetDeleteServerHistoryScopeInfoCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetUpdateClientScopeInfoCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetUpdateServerScopeInfoCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetUpdateServerHistoryScopeInfoCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetLocalTimestampCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetDropClientScopeInfoTableCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetDropServerScopeInfoTableCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetDropServerHistoryScopeInfoTableCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetExistsClientScopeInfoCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetExistsServerScopeInfoCommand(DbConnection connection, DbTransaction transaction);
        public abstract DbCommand GetExistsServerHistoryScopeInfoCommand(DbConnection connection, DbTransaction transaction);



        // Internal commands cache
        private ConcurrentDictionary<string, Lazy<SyncCommand>> commands = new();

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
                DbScopeCommandType.GetAllClientScopesInfo => GetAllClientScopesInfoCommand(connection, transaction),
                DbScopeCommandType.GetAllServerScopesInfo => GetAllServerScopesInfoCommand(connection, transaction),
                DbScopeCommandType.GetAllServerHistoryScopesInfo => GetAllServerHistoriesScopesInfoCommand(connection, transaction),

                DbScopeCommandType.GetClientScopeInfo => GetClientScopeInfoCommand( connection, transaction),
                DbScopeCommandType.GetServerScopeInfo => GetServerScopeInfoCommand(connection, transaction),
                DbScopeCommandType.GetServerHistoryScopeInfo => GetServerHistoryScopeInfoCommand(connection, transaction),

                DbScopeCommandType.CreateClientScopeInfoTable => GetCreateClientScopeInfoTableCommand( connection, transaction),
                DbScopeCommandType.CreateServerScopeInfoTable => GetCreateServerScopeInfoTableCommand(connection, transaction),
                DbScopeCommandType.CreateServerHistoryScopeInfoTable => GetCreateServerHistoryScopeInfoTableCommand(connection, transaction),

                DbScopeCommandType.ExistsClientScopeInfoTable => GetExistsClientScopeInfoTableCommand(connection, transaction),
                DbScopeCommandType.ExistsServerScopeInfoTable => GetExistsServerScopeInfoTableCommand(connection, transaction),
                DbScopeCommandType.ExistsServerHistoryScopeInfoTable => GetExistsServerHistoryScopeInfoTableCommand(connection, transaction),

                DbScopeCommandType.InsertClientScopeInfo => GetInsertClientScopeInfoCommand(connection, transaction),
                DbScopeCommandType.InsertServerScopeInfo => GetInsertServerScopeInfoCommand(connection, transaction),
                DbScopeCommandType.InsertServerHistoryScopeInfo => GetInsertServerHistoryScopeInfoCommand(connection, transaction),

                DbScopeCommandType.UpdateClientScopeInfo => GetUpdateClientScopeInfoCommand(connection, transaction),
                DbScopeCommandType.UpdateServerScopeInfo => GetUpdateServerScopeInfoCommand(connection, transaction),
                DbScopeCommandType.UpdateServerHistoryScopeInfo => GetUpdateServerHistoryScopeInfoCommand(connection, transaction),

                DbScopeCommandType.DeleteClientScopeInfo => GetDeleteClientScopeInfoCommand(connection, transaction),
                DbScopeCommandType.DeleteServerScopeInfo => GetDeleteServerScopeInfoCommand(connection, transaction),
                DbScopeCommandType.DeleteServerHistoryScopeInfo => GetDeleteServerHistoryScopeInfoCommand(connection, transaction),

                DbScopeCommandType.ExistClientScopeInfo => GetExistsClientScopeInfoCommand(connection, transaction),
                DbScopeCommandType.ExistServerScopeInfo => GetExistsServerScopeInfoCommand(connection, transaction),
                DbScopeCommandType.ExistServerHistoryScopeInfo => GetExistsServerHistoryScopeInfoCommand(connection, transaction),

                DbScopeCommandType.DropClientScopeInfoTable => GetDropClientScopeInfoTableCommand(connection, transaction),
                DbScopeCommandType.DropServerScopeInfoTable => GetDropServerScopeInfoTableCommand(connection, transaction),
                DbScopeCommandType.DropServerHistoryScopeInfoTable => GetDropServerHistoryScopeInfoTableCommand(connection, transaction),

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
            var lazyCommand = commands.GetOrAdd(commandKey, k => new Lazy<SyncCommand>(() =>
            {
                var syncCommand = new SyncCommand(commandKey);
                return syncCommand;
            }));

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
