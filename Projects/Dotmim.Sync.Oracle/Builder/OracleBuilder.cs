using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync.Oracle.Builder
{
    public class OracleBuilder : DbBuilder
    {
        public OracleBuilder(DmTable tableDescription) 
            : base(tableDescription)
        {
            useBulkProcedures = false;
        }

        internal static (ObjectNameParser tableName, ObjectNameParser trackingName) GetParsers(DmTable tableDescription)
        {
            string tableAndPrefixName = String.IsNullOrWhiteSpace(tableDescription.Schema) ? tableDescription.TableName : $"{tableDescription.Schema}.{tableDescription.TableName}";
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
            stringBuilder.Append(string.Concat("-- END ", commentText, str, "\n"));
            return stringBuilder.ToString();
        }

        public override IDbBuilderProcedureHelper CreateProcBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new OracleBuilderProcedure(TableDescription, connection, transaction);
        }

        public override DbSyncAdapter CreateSyncAdapter(DbConnection connection, DbTransaction transaction = null)
        {
            return new OracleSyncAdapter(TableDescription, connection, transaction);
        }

        public override IDbBuilderTableHelper CreateTableBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new OracleBuilderTable(TableDescription, connection, transaction);
        }

        public override IDbBuilderTrackingTableHelper CreateTrackingTableBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new OracleBuilderTrackingTable(TableDescription, connection, transaction);
        }

        public override IDbBuilderTriggerHelper CreateTriggerBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            return new OracleBuilderTrigger(TableDescription, connection, transaction);
        }
    }
}
