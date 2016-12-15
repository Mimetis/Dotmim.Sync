using Dotmim.Sync.Core.Builders;
using System;
using System.Collections.Generic;
using System.Text;
using Dotmim.Sync.Data;
using System.Data.Common;

namespace Dotmim.Sync.SqlServer.Builders
{

    /// <summary>
    /// The SqlBuilder class is the Sql implementation of DbBuilder class.
    /// In charge of creating tracking table, stored proc and triggers.
    /// </summary>
    public class SqlBuilder : DbBuilder
    {

        DmTable table;
        DbConnection connection;
        SqlBuilderProcedure builderProc;
        SqlBuilderTable builderTable;
        SqlBuilderTrackingTable builderTrackingTable;
        SqlBuilderTriggerHelper builderTrigger;

        public SqlBuilder(DmTable table, DbConnection connection, DbBuilderOption option = DbBuilderOption.Create) 
            : base(table, connection, option)
        {
            this.table = table;
            this.connection = connection;
            this.builderProc = new SqlBuilderProcedure(table);
            this.builderTable = new SqlBuilderTable(table);
            this.builderTrackingTable = new SqlBuilderTrackingTable(table);
            this.builderTrigger = new SqlBuilderTriggerHelper(table);

        }

        public override IDbBuilderProcedureHelper ProcBuilder => this.builderProc;

        public override IDbBuilderTableHelper TableBuilder => this.builderTable;

        public override IDbBuilderTrackingTableHelper TrackingTableBuilder => this.builderTrackingTable;

        public override IDbBuilderTriggerHelper TriggerBuilder => this.builderTrigger;


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
            stringBuilder.Append(string.Concat(str, (includeGo ? string.Concat("GO", str) : string.Empty)));
            stringBuilder.Append(string.Concat("-- END ", commentText, str, "\n"));
            return stringBuilder.ToString();
        }
    }
}
