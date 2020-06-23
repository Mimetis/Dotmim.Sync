using Dotmim.Sync.Builders;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync.Postgres.Builders
{

    /// <summary>
    /// The SqlBuilder class is the Sql implementation of DbBuilder class.
    /// In charge of creating tracking table, stored proc, triggers and adapters.
    /// </summary>
    public class NpgsqlTableBuilder : DbTableBuilder
    {

        public NpgsqlObjectNames ObjectNames { get; private set; }

        public NpgsqlTableBuilder(SyncTable tableDescription, SyncSetup setup) : base(tableDescription, setup)
            => this.ObjectNames = new NpgsqlObjectNames(tableDescription, setup);

        public override (ParserName tableName, ParserName trackingName) GetParsers(SyncTable tableDescription, SyncSetup setup)
        {
            var originalTableName = ParserName.Parse(tableDescription, "\"");

            var pref = setup.TrackingTablesPrefix;
            var suf = setup.TrackingTablesSuffix;

            // be sure, at least, we have a suffix if we have empty values. 
            // othewise, we have the same name for both table and tracking table
            if (string.IsNullOrEmpty(pref) && string.IsNullOrEmpty(suf))
                suf = "_tracking";

            var trakingTableNameString = $"{pref}{originalTableName.ObjectName}{suf}";

            if (!string.IsNullOrEmpty(originalTableName.SchemaName))
                trakingTableNameString = $"{originalTableName.SchemaName}.{trakingTableNameString}";

            var trackingTableName = ParserName.Parse(trakingTableNameString, "\"");

            return (originalTableName, trackingTableName);
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
            stringBuilder.Append(string.Concat(str, (includeGo ? string.Concat("GO", str) : string.Empty)));
            stringBuilder.Append(string.Concat("-- END ", commentText, str, "\n"));
            return stringBuilder.ToString();
        }

        public override IDbBuilderProcedureHelper CreateProcBuilder()
        {
            return new NpgsqlBuilderProcedure(TableDescription, this.TableName, this.TrackingTableName, Setup);
        }

        public override IDbBuilderTriggerHelper CreateTriggerBuilder()
        {
            return new SqlBuilderTrigger(TableDescription, this.TableName, this.TrackingTableName, Setup);
        }

        public override IDbBuilderTableHelper CreateTableBuilder()
        {
            return new NpgsqlBuilderTable(TableDescription, this.TableName, this.TrackingTableName, Setup);
        }

        public override IDbBuilderTrackingTableHelper CreateTrackingTableBuilder()
        {
            return new SqlBuilderTrackingTable(TableDescription, this.TableName, this.TrackingTableName, Setup);
        }

        public override SyncAdapter CreateSyncAdapter()
        {
            return new NpgsqlSyncAdapter(TableDescription, this.TableName, this.TrackingTableName, Setup);
        }
    }
}
