using Dotmim.Sync.Builders;
using System.Text;

using System.Data.Common;
using System.Threading.Tasks;
using System.Collections.Generic;
using Dotmim.Sync.Manager;

namespace Dotmim.Sync.Sqlite
{

    /// <summary>
    /// The SqlBuilder class is the Sql implementation of DbBuilder class.
    /// In charge of creating tracking table, stored proc, triggers and adapters.
    /// </summary>
    public class SqliteTableBuilder : DbTableBuilder
    {

        SqliteObjectNames sqlObjectNames;

        public SqliteTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup) : base(tableDescription, tableName, trackingTableName, setup)
        {
            sqlObjectNames = new SqliteObjectNames(tableDescription, this.TableName, this.TrackingTableName, setup);
        }

        public static string WrapScriptTextWithComments(string commandText, string commentText, bool includeGo = true, int indentLevel = 0)
        {
            var stringBuilder = new StringBuilder();
            var stringBuilder1 = new StringBuilder("\n");
            for (int i = 0; i < indentLevel; i++)
            {
                stringBuilder.Append("\t");
                stringBuilder1.Append("\t");
            }
            string str = stringBuilder1.ToString();
            stringBuilder.Append(string.Concat("-- BEGIN ", commentText, str));
            stringBuilder.Append(commandText);
            stringBuilder.Append(string.Concat(str, includeGo ? string.Concat("GO;", str) : string.Empty));
            stringBuilder.Append(string.Concat("-- END ", commentText, str, "\n"));
            return stringBuilder.ToString();
        }

        
        public override Task<DbCommand> GetCreateSchemaCommandAsync(DbConnection connection, DbTransaction transaction) => throw new System.NotImplementedException();
        public override Task<DbCommand> GetCreateTableCommandAsync(DbConnection connection, DbTransaction transaction) => throw new System.NotImplementedException();
        public override Task<DbCommand> GetExistsTableCommandAsync(DbConnection connection, DbTransaction transaction) => throw new System.NotImplementedException();
        public override Task<DbCommand> GetExistsSchemaCommandAsync(DbConnection connection, DbTransaction transaction) => throw new System.NotImplementedException();
        public override Task<DbCommand> GetDropTableCommandAsync(DbConnection connection, DbTransaction transaction) => throw new System.NotImplementedException();
        public override Task<DbCommand> GetExistsStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, DbConnection connection, DbTransaction transaction) => throw new System.NotImplementedException();
        public override Task<DbCommand> GetCreateStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction) => throw new System.NotImplementedException();
        public override Task<DbCommand> GetDropStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction) => throw new System.NotImplementedException();
        public override Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction) => throw new System.NotImplementedException();
        public override Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction) => throw new System.NotImplementedException();
        public override Task<DbCommand> GetRenameTrackingTableCommandAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction) => throw new System.NotImplementedException();
        public override Task<DbCommand> GetExistsTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction) => throw new System.NotImplementedException();
        public override Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction) => throw new System.NotImplementedException();
        public override Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction) => throw new System.NotImplementedException();
        public override Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction) => throw new System.NotImplementedException();
        public override Task<IEnumerable<SyncColumn>> GetColumnsAsync(DbConnection connection, DbTransaction transaction) => throw new System.NotImplementedException();
        public override Task<IEnumerable<DbRelationDefinition>> GetRelationsAsync(DbConnection connection, DbTransaction transaction) => throw new System.NotImplementedException();
        public override Task<IEnumerable<SyncColumn>> GetPrimaryKeysAsync(DbConnection connection, DbTransaction transaction) => throw new System.NotImplementedException();
    }
}
