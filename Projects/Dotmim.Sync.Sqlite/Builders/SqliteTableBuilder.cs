using Dotmim.Sync.Builders;
using System.Text;

using System.Data.Common;


namespace Dotmim.Sync.Sqlite
{

    /// <summary>
    /// The SqlBuilder class is the Sql implementation of DbBuilder class.
    /// In charge of creating tracking table, stored proc, triggers and adapters.
    /// </summary>
    public class SqliteTableBuilder : DbTableBuilder
    {

        SqliteObjectNames sqlObjectNames;

        public SqliteTableBuilder(SyncTable tableDescription, SyncSetup setup) : base(tableDescription, setup)
        {
            sqlObjectNames = new SqliteObjectNames(tableDescription, setup);
        }

        internal static (ParserName tableName, ParserName trackingName) GetParsers(SyncTable tableDescription, SyncSetup setup)
        {
            string tableAndPrefixName = tableDescription.TableName;
            var originalTableName = ParserName.Parse(tableDescription);

            var pref = setup.TrackingTablesPrefix != null ? setup.TrackingTablesPrefix : "";
            var suf = setup.TrackingTablesSuffix != null ? setup.TrackingTablesSuffix : "";

            // be sure, at least, we have a suffix if we have empty values. 
            // othewise, we have the same name for both table and tracking table
            if (string.IsNullOrEmpty(pref) && string.IsNullOrEmpty(suf))
                suf = "_tracking";

            var trackingTableName = ParserName.Parse($"{pref}{tableAndPrefixName}{suf}");

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
            stringBuilder.Append(string.Concat(str, includeGo ? string.Concat("GO;", str) : string.Empty));
            stringBuilder.Append(string.Concat("-- END ", commentText, str, "\n"));
            return stringBuilder.ToString();
        }

        /// <summary>
        /// Proc are not supported in Sqlite
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="transaction"></param>
        /// <returns></returns>
        public override IDbBuilderProcedureHelper CreateProcBuilder(DbConnection connection, DbTransaction transaction = null) => null;

        public override IDbBuilderTriggerHelper CreateTriggerBuilder(DbConnection connection, DbTransaction transaction = null) 
            => new SqliteBuilderTrigger(TableDescription, Setup, connection, transaction);

        public override IDbBuilderTableHelper CreateTableBuilder(DbConnection connection, DbTransaction transaction = null) 
            => new SqliteBuilderTable(TableDescription, Setup, connection, transaction);

        public override IDbBuilderTrackingTableHelper CreateTrackingTableBuilder(DbConnection connection, DbTransaction transaction = null) 
            => new SqliteBuilderTrackingTable(TableDescription, Setup, connection, transaction);

        public override DbSyncAdapter CreateSyncAdapter(DbConnection connection, DbTransaction transaction = null) 
            => new SqliteSyncAdapter(TableDescription, Setup, connection, transaction);
    }
}
