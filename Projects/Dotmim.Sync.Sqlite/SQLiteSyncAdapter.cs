using System;
using System.Collections.Generic;
using System.Linq;

using System.Data.Common;
using System.Data;
using Dotmim.Sync.Builders;
using Microsoft.Data.Sqlite;
using System.Threading.Tasks;

namespace Dotmim.Sync.Sqlite
{
    public class SqliteSyncAdapter : DbSyncAdapter
    {
        private SqliteObjectNames sqliteObjectNames;

        public override bool SupportsOutputParameters => false;

        public SqliteSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup, string scopeName) : base(tableDescription, setup, scopeName)
        {
            this.sqliteObjectNames = new SqliteObjectNames(this.TableDescription, tableName, trackingName, this.Setup, scopeName);
        }

        public override (DbCommand, bool) GetCommand(SyncContext context, DbCommandType commandType, SyncFilter filter = null)
        {
            var command = new SqliteCommand();
            string text;
            text = this.sqliteObjectNames.GetCommandName(commandType, filter);

            // on Sqlite, everything is text :)
            command.CommandType = CommandType.Text;
            command.CommandText = text;

            return (command, false);
        }

        public override void AddCommandParameterValue(SyncContext context, DbParameter parameter, object value, DbCommand command, DbCommandType commandType)
        {
            if (value == null || value == DBNull.Value)
                parameter.Value = DBNull.Value;
            else
                parameter.Value = SyncTypeConverter.TryConvertFromDbType(value, parameter.DbType);
        }

        public override DbCommand EnsureCommandParametersValues(SyncContext context, DbCommand command, DbCommandType commandType, DbConnection connection, DbTransaction transaction)
        {

            return command;
        }

        public override Task ExecuteBatchCommandAsync(SyncContext context, DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> applyRows, SyncTable schemaChangesTable, SyncTable failedRows, long? lastTimestamp, DbConnection connection, DbTransaction transaction = null)
            => throw new NotImplementedException();


        private DbType GetValidDbType(DbType dbType)
        {
            if (dbType == DbType.Time)
                return DbType.String;

            if (dbType == DbType.Object)
                return DbType.String;

            return dbType;
        }
    }
}
