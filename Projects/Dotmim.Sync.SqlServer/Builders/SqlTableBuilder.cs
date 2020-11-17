using Dotmim.Sync.Builders;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync.SqlServer.Builders
{

    /// <summary>
    /// The SqlBuilder class is the Sql implementation of DbBuilder class.
    /// In charge of creating tracking table, stored proc, triggers and adapters.
    /// </summary>
    public class SqlTableBuilder : DbTableBuilder
    {

        public SqlTableBuilder(SyncTable tableDescription, SyncSetup setup) : base(tableDescription, setup)
        {
        }

        public override (ParserName tableName, ParserName trackingName) GetParsers(SyncTable tableDescription, SyncSetup setup)
        {
            var sqlObjectNames = new SqlObjectNames(tableDescription, setup);
            return (sqlObjectNames.GetTableName(), sqlObjectNames.GetTrackingTableName());
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
            stringBuilder.Append(string.Concat(str, includeGo ? string.Concat("GO", str) : string.Empty));
            stringBuilder.Append(string.Concat("-- END ", commentText, str, "\n"));
            return stringBuilder.ToString();
        }

        // TODO : Virer table name et tracking name
        public override IDbBuilderTriggerHelper CreateTriggerBuilder() 
            => new SqlBuilderTrigger(TableDescription, this.TableName, this.TrackingTableName, Setup);

        // TODO : Virer table name et tracking name
        public override IDbBuilderTableHelper CreateTableBuilder() 
            => new SqlBuilderTable(TableDescription, this.TableName, this.TrackingTableName, Setup);

        // TODO : Virer table name et tracking name
        public override IDbBuilderTrackingTableHelper CreateTrackingTableBuilder() 
            => new SqlBuilderTrackingTable(TableDescription, this.TableName, this.TrackingTableName, Setup);

        // TODO : Virer table name et tracking name
        public override SyncAdapter CreateSyncAdapter() 
            => new SqlSyncAdapter(TableDescription, this.TableName, this.TrackingTableName, Setup);
    }
}
