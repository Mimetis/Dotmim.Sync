using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dotmim.Sync.Core.Scope;
using Dotmim.Sync.Data;
using System.Data.Common;
using Dotmim.Sync.Core.Log;
using System.Data;
using System.Data.SqlTypes;
using System.Reflection;
using Dotmim.Sync.Core.Builders;
using Dotmim.Sync.Core.Common;
using System.Data.SQLite;

namespace Dotmim.Sync.SQLite
{
    public class SQLiteSyncAdapter : DbSyncAdapter
    {
        private SQLiteConnection connection;
        private SQLiteTransaction transaction;
        private SQLiteObjectNames sqliteObjectNames;

        public override DbConnection Connection
        {
            get
            {
                return this.connection;
            }
        }
        public override DbTransaction Transaction
        {
            get
            {
                return this.transaction;
            }

        }

        public SQLiteSyncAdapter(DmTable tableDescription, DbConnection connection, DbTransaction transaction) : base(tableDescription)
        {
            var sqlc = connection as SQLiteConnection;
            this.connection = sqlc ?? throw new InvalidCastException("Connection should be a SQLiteConnection");

            this.transaction = transaction as SQLiteTransaction;

            this.sqliteObjectNames = new SQLiteObjectNames(TableDescription);
        }

        public override bool IsPrimaryKeyViolation(Exception Error)
        {
            throw new NotImplementedException();
        }

        public override DbCommand GetCommand(DbCommandType commandType)
        {
            var command = this.Connection.CreateCommand();

            // on Sqlite, everything is text :)
            command.CommandType = CommandType.Text;
            command.CommandText = this.sqliteObjectNames.GetCommandName(commandType);
            command.Connection = Connection;

            if (Transaction != null)
                command.Transaction = Transaction;

            return command;
        }

        public override void SetCommandParameters(DbCommand command)
        {
            throw new NotImplementedException();
        }

        public override void ExecuteBatchCommand(DbCommand cmd, DmTable applyTable, DmTable failedRows, ScopeInfo scope)
        {
            throw new NotImplementedException();
        }
    }
}
