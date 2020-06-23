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

        public SqlObjectNames ObjectNames { get; private set; }

        public SqlTableBuilder(SyncTable tableDescription, SyncSetup setup) : base(tableDescription, setup)
        {
            this.ObjectNames = new SqlObjectNames(tableDescription, this.TableName, this.TrackingTableName, setup);
        }

        public override (ParserName tableName, ParserName trackingName) GetParsers(SyncTable tableDescription, SyncSetup setup)
        {
            var originalTableName = ParserName.Parse(tableDescription);

            var pref = setup.TrackingTablesPrefix;
            var suf = setup.TrackingTablesSuffix;

            // be sure, at least, we have a suffix if we have empty values. 
            // othewise, we have the same name for both table and tracking table
            if (string.IsNullOrEmpty(pref) && string.IsNullOrEmpty(suf))
                suf = "_tracking";

            var trakingTableNameString = $"{pref}{originalTableName.ObjectName}{suf}";

            if (!string.IsNullOrEmpty(originalTableName.SchemaName))
                trakingTableNameString = $"{originalTableName.SchemaName}.{trakingTableNameString}";

            var trackingTableName = ParserName.Parse(trakingTableNameString);

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

        public override IDbBuilderProcedureHelper CreateProcBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new SqlBuilderProcedure(TableDescription, this.TableName, this.TrackingTableName, Setup, connection, transaction);
        }

        public override IDbBuilderTriggerHelper CreateTriggerBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new SqlBuilderTrigger(TableDescription, this.TableName, this.TrackingTableName, Setup, connection, transaction);
        }

        public override IDbBuilderTableHelper CreateTableBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new SqlBuilderTable(TableDescription, this.TableName, this.TrackingTableName, Setup, connection, transaction);
        }

        public override IDbBuilderTrackingTableHelper CreateTrackingTableBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new SqlBuilderTrackingTable(TableDescription, this.TableName, this.TrackingTableName, Setup, connection, transaction);
        }

        public override DbSyncAdapter CreateSyncAdapter()
        {
            return new SqlSyncAdapter(TableDescription, this.TableName, this.TrackingTableName, Setup);
        }
    }
}
