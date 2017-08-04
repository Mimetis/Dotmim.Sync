using Dotmim.Sync.Core.Builders;
using System;
using System.Collections.Generic;
using System.Text;
using Dotmim.Sync.Data;
using System.Data.Common;
using Dotmim.Sync.Core.Common;


namespace Dotmim.Sync.SQLite
{

    /// <summary>
    /// The SqlBuilder class is the Sql implementation of DbBuilder class.
    /// In charge of creating tracking table, stored proc, triggers and adapters.
    /// </summary>
    public class SQLiteBuilder : DbBuilder
    {

        SQLiteObjectNames sqlObjectNames;
       
        public SQLiteBuilder(DmTable tableDescription, DbBuilderOption option = DbBuilderOption.CreateOrUseExistingSchema)
            : base(tableDescription, option)
        {
            sqlObjectNames = new SQLiteObjectNames(tableDescription);
        }

        internal static (ObjectNameParser tableName, ObjectNameParser trackingName) GetParsers(DmTable tableDescription)
        {
            string tableAndPrefixName = tableDescription.TableName;
            var originalTableName = new ObjectNameParser(tableAndPrefixName, "[", "]");
            var trackingTableName = new ObjectNameParser($"{tableAndPrefixName}_tracking", "[", "]");

            return (originalTableName, trackingTableName);
        }
        public static string WrapScriptTextWithComments(string commandText, string commentText, bool includeGo = true, int indentLevel = 0)
        {
            StringBuilder stringBuilder = new StringBuilder();
            StringBuilder stringBuilder1 = new StringBuilder("\n");
            for (int i = 0; i < indentLevel; i++)
            {
                stringBuilder.Append("\t");
                stringBuilder1.Append("\t");
            }
            string str = stringBuilder1.ToString();
            stringBuilder.Append(string.Concat("-- BEGIN ", commentText, str));
            stringBuilder.Append(commandText);
            stringBuilder.Append(string.Concat(str, (includeGo ? string.Concat("GO;", str) : string.Empty)));
            stringBuilder.Append(string.Concat("-- END ", commentText, str, "\n"));
            return stringBuilder.ToString();
        }

        /// <summary>
        /// Proc are not supported in SQLite
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="transaction"></param>
        /// <returns></returns>
        public override IDbBuilderProcedureHelper CreateProcBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return null;
        }

        public override IDbBuilderTriggerHelper CreateTriggerBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new SQLiteBuilderTrigger(TableDescription, connection, transaction);
        }

        public override IDbBuilderTableHelper CreateTableBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new SQLiteBuilderTable(TableDescription, connection, transaction);
        }

        public override IDbBuilderTrackingTableHelper CreateTrackingTableBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new SQLiteBuilderTrackingTable(TableDescription, connection, transaction);
        }

        public override DbSyncAdapter CreateSyncAdapter(DbConnection connection, DbTransaction transaction = null)
        {
            return new SQLiteSyncAdapter(TableDescription, connection, transaction);
        }
    }
}
