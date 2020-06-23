using Dotmim.Sync.Builders;
using System.Text;

using System.Data.Common;

namespace Dotmim.Sync.MySql
{

    /// <summary>
    /// The MySqlBuilder class is the MySql implementation of DbBuilder class.
    /// In charge of creating tracking table, stored proc, triggers and adapters.
    /// </summary>
    public class MyTableSqlBuilder : DbTableBuilder
    {

        MySqlObjectNames sqlObjectNames;

        public MyTableSqlBuilder(SyncTable tableDescription, SyncSetup setup) : base(tableDescription, setup)
        {

            sqlObjectNames = new MySqlObjectNames(tableDescription, this.TableName, this.TrackingTableName, setup);
        }

        public override (ParserName tableName, ParserName trackingName) GetParsers(SyncTable tableDescription, SyncSetup setup)
        {
            string tableAndPrefixName = tableDescription.TableName;

            var originalTableName = ParserName.Parse(tableDescription, "`");

            var pref = setup.TrackingTablesPrefix != null ? setup.TrackingTablesPrefix : "";
            var suf = setup.TrackingTablesSuffix != null ? setup.TrackingTablesSuffix : "";

            // be sure, at least, we have a suffix if we have empty values. 
            // othewise, we have the same name for both table and tracking table
            if (string.IsNullOrEmpty(pref) && string.IsNullOrEmpty(suf))
                suf = "_tracking";

            var trackingTableName = ParserName.Parse($"{pref}{tableAndPrefixName}{suf}", "`");

            return (originalTableName, trackingTableName);
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

        public override IDbBuilderProcedureHelper CreateProcBuilder() => new MySqlBuilderProcedure(TableDescription, this.TableName, this.TrackingTableName, Setup);

        public override IDbBuilderTriggerHelper CreateTriggerBuilder() => new MySqlBuilderTrigger(TableDescription, this.TableName, this.TrackingTableName, Setup);

        public override IDbBuilderTableHelper CreateTableBuilder() => new MySqlBuilderTable(TableDescription, this.TableName, this.TrackingTableName, Setup);

        public override IDbBuilderTrackingTableHelper CreateTrackingTableBuilder() => new MySqlBuilderTrackingTable(TableDescription, this.TableName, this.TrackingTableName, Setup);

        public override DbSyncAdapter CreateSyncAdapter() => new MySqlSyncAdapter(TableDescription, this.TableName, this.TrackingTableName, Setup);
    }
}
