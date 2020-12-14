using Dotmim.Sync.Builders;
using System.Text;

using System.Data.Common;
using System.Threading.Tasks;
using System.Collections.Generic;
using Dotmim.Sync.Manager;

namespace Dotmim.Sync.MySql
{

    /// <summary>
    /// The MySqlBuilder class is the MySql implementation of DbBuilder class.
    /// In charge of creating tracking table, stored proc, triggers and adapters.
    /// </summary>
    public class MyTableSqlBuilder : DbTableBuilder
    {

        MySqlObjectNames sqlObjectNames;

        public MyTableSqlBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup) : base(tableDescription, tableName, trackingTableName, setup)
        {

            sqlObjectNames = new MySqlObjectNames(tableDescription, this.TableName, this.TrackingTableName, setup);
        }

        
        public static string WrapScriptTextWithComments(string commandText, string commentText)
        {
            var stringBuilder = new StringBuilder();
            var stringBuilder1 = new StringBuilder("\n");

            string str = stringBuilder1.ToString();
            stringBuilder.AppendLine("DELIMITER $$ ");
            stringBuilder.Append(string.Concat("-- BEGIN ", commentText, str));
            stringBuilder.Append(commandText);
            stringBuilder.Append(string.Concat("-- END ", commentText, str, "\n"));
            stringBuilder.AppendLine("$$ ");
            stringBuilder.AppendLine("DELIMITER ;");
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
