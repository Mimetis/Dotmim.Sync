using Dotmim.Sync.Builders;
using System.Text;
using Dotmim.Sync.Data;
using System.Data.Common;

namespace Dotmim.Sync.MySql
{

    /// <summary>
    /// The MySqlBuilder class is the MySql implementation of DbBuilder class.
    /// In charge of creating tracking table, stored proc, triggers and adapters.
    /// </summary>
    public class MySqlBuilder : DbBuilder
    {

        MySqlObjectNames sqlObjectNames;
       
        public MySqlBuilder(DmTable tableDescription) : base(tableDescription)
        {
            sqlObjectNames = new MySqlObjectNames(tableDescription);
        }

        internal static (ObjectNameParser tableName, ObjectNameParser trackingName) GetParsers(DmTable tableDescription)
        {
            string tableAndPrefixName = tableDescription.TableName;
            var originalTableName = new ObjectNameParser(tableAndPrefixName, "`", "`");

            var pref = tableDescription.TrackingTablesPrefix != null ? tableDescription.TrackingTablesPrefix : "";
            var suf = tableDescription.TrackingTablesSuffix != null ? tableDescription.TrackingTablesSuffix : "";

            // be sure, at least, we have a suffix if we have empty values. 
            // othewise, we have the same name for both table and tracking table
            if (string.IsNullOrEmpty(pref) && string.IsNullOrEmpty(suf))
                suf = "_tracking";

            var trackingTableName = new ObjectNameParser($"{pref}{tableAndPrefixName}{suf}", "`", "`");

            return (originalTableName, trackingTableName);
        }
        public static string WrapScriptTextWithComments(string commandText, string commentText)
        {
            StringBuilder stringBuilder = new StringBuilder();
            StringBuilder stringBuilder1 = new StringBuilder("\n");

            string str = stringBuilder1.ToString();
            stringBuilder.AppendLine("DELIMITER $$ ");
            stringBuilder.Append(string.Concat("-- BEGIN ", commentText, str));
            stringBuilder.Append(commandText);
            stringBuilder.Append(string.Concat("-- END ", commentText, str, "\n"));
            stringBuilder.AppendLine("$$ ");
            stringBuilder.AppendLine("DELIMITER ;");
            return stringBuilder.ToString();
        }

         public override IDbBuilderProcedureHelper CreateProcBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new MySqlBuilderProcedure(TableDescription, connection, transaction);
        }

        public override IDbBuilderTriggerHelper CreateTriggerBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new MySqlBuilderTrigger(TableDescription, connection, transaction);
        }

        public override IDbBuilderTableHelper CreateTableBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new MySqlBuilderTable(TableDescription, connection, transaction);
        }

        public override IDbBuilderTrackingTableHelper CreateTrackingTableBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new MySqlBuilderTrackingTable(TableDescription, connection, transaction);
        }

        public override DbSyncAdapter CreateSyncAdapter(DbConnection connection, DbTransaction transaction = null)
        {
            return new MySqlSyncAdapter(TableDescription, connection, transaction);
        }
    }
}
