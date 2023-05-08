using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient.Server;
using Dotmim.Sync.SqlServer.Builders;
using System.Text;

namespace Dotmim.Sync.SqlServer
{
    public class SqlChangeTrackingSyncAdapter : SqlSyncAdapter
    {
        private readonly ParserName tableName;
        private readonly ParserName trackingName;


        public SqlChangeTrackingSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup, string scopeName, bool useBulkOperations) 
            : base(tableDescription, tableName, trackingName, setup, scopeName, useBulkOperations)
        {
            this.tableName = tableName;
            this.trackingName = trackingName;

        }

        /// <summary>
        /// Overriding adapter since the update metadata is not a stored proc that we can override
        /// </summary>
        public override (DbCommand, bool) GetCommand(DbCommandType nameType, SyncFilter filter)
        {
            if (nameType == DbCommandType.UpdateMetadata)
            {
                var c = new SqlCommand("Set @sync_row_count = 1;");
                c.Parameters.Add("@sync_row_count", SqlDbType.Int);
                return (c, false);
            }

            if (nameType == DbCommandType.SelectRow)
            {
                return (BuildSelectInitializedChangesCommand(), false);
            }

            return base.GetCommand(nameType, filter);
        }

        private SqlCommand BuildSelectInitializedChangesCommand()
        {
            var sqlCommand = new SqlCommand();
            var stringBuilder1 = new StringBuilder();

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine(";WITH ");
            stringBuilder.Append("  ").Append(this.trackingName.Quoted()).AppendLine(" AS (");
            stringBuilder.Append("\tSELECT ");
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.Append("[CT].").Append(columnName).Append(", ");
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("\tCAST([CT].[SYS_CHANGE_CONTEXT] as uniqueidentifier) AS [sync_update_scope_id], ");
            stringBuilder.AppendLine("\t[CT].[SYS_CHANGE_VERSION] as [sync_timestamp],");
            stringBuilder.AppendLine("\tCASE WHEN [CT].[SYS_CHANGE_OPERATION] = 'D' THEN 1 ELSE 0 END AS [sync_row_is_tombstone]");
            stringBuilder.Append("\tFROM CHANGETABLE(CHANGES ").Append(tableName.Schema().Quoted()).AppendLine(", NULL) AS [CT]");
            stringBuilder.AppendLine("\t)");

                stringBuilder.AppendLine("SELECT ");

            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                var isPrimaryKey = this.TableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.Append("\t[side].").Append(columnName).AppendLine(", ");
                else
                    stringBuilder.Append("\t[base].").Append(columnName).AppendLine(", ");

            }
            stringBuilder.AppendLine($"\t[side].[sync_row_is_tombstone] as [sync_row_is_tombstone], ");
            stringBuilder.AppendLine($"\t[side].[sync_update_scope_id] as [sync_update_scope_id] ");
            stringBuilder.Append("FROM ").Append(tableName.Schema().Quoted()).AppendLine(" [base]");
            stringBuilder.Append("RIGHT JOIN ").Append(trackingName.Quoted()).Append(" [side] ");
            stringBuilder.Append("ON ");

            string empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();

                stringBuilder.Append(empty).Append("[base].").Append(columnName).Append(" = [side].").Append(columnName);
                stringBuilder1.Append(empty).Append("[side].").Append(columnName).Append(" = @").Append(parameterName);
                empty = " AND ";
            }

            stringBuilder.AppendLine();
            stringBuilder.Append(string.Concat("WHERE ", stringBuilder1.ToString()));

            sqlCommand.CommandText = stringBuilder.ToString();

            return sqlCommand;
        }



    }
}
