using Dotmim.Sync.Builders;
using Dotmim.Sync.SqlServer.Builders;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Data.Common;
using System.Linq;
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
        public override (DbCommand, bool) GetCommand(DbConnection connection, DbCommandType commandType, SyncFilter filter)
        {
            var cacheKey = GetCacheKey(connection, commandType, filter);

            if (commandCache.TryGetValue(cacheKey, out var result))
                return result;

            if (commandType == DbCommandType.UpdateMetadata)
            {
                var c = new SqlCommand("Set @sync_row_count = 1;");
                c.Parameters.Add("@sync_row_count", SqlDbType.Int);
                return (c, false);
            }

            if (commandType == DbCommandType.SelectRow)
            {
                return (BuildSelectInitializedChangesCommand(), false);
            }

            var (command, isBatch) = base.GetCommand(connection, commandType, filter);

            commandCache.TryAdd(cacheKey, (command, isBatch));

            return (command, isBatch);
        }

        private SqlCommand BuildSelectInitializedChangesCommand()
        {
            var sqlCommand = new SqlCommand();
            var stringBuilder1 = new StringBuilder();

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine(";WITH ");
            stringBuilder.AppendLine($"  {this.trackingName.Quoted()} AS (");
            stringBuilder.Append("\tSELECT ");
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.Append($"[CT].{columnName}, ");
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("\tCAST([CT].[SYS_CHANGE_CONTEXT] as uniqueidentifier) AS [sync_update_scope_id], ");
            stringBuilder.AppendLine("\t[CT].[SYS_CHANGE_VERSION] as [sync_timestamp],");
            stringBuilder.AppendLine("\tCASE WHEN [CT].[SYS_CHANGE_OPERATION] = 'D' THEN 1 ELSE 0 END AS [sync_row_is_tombstone]");
            stringBuilder.AppendLine($"\tFROM CHANGETABLE(CHANGES {tableName.Schema().Quoted()}, NULL) AS [CT]");
            stringBuilder.AppendLine("\t)");

                stringBuilder.AppendLine("SELECT ");

            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                var isPrimaryKey = this.TableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.AppendLine($"\t[side].{columnName}, ");
                else
                    stringBuilder.AppendLine($"\t[base].{columnName}, ");

            }
            stringBuilder.AppendLine($"\t[side].[sync_row_is_tombstone] as [sync_row_is_tombstone], ");
            stringBuilder.AppendLine($"\t[side].[sync_update_scope_id] as [sync_update_scope_id] ");
            stringBuilder.AppendLine($"FROM {tableName.Schema().Quoted()} [base]");
            stringBuilder.Append($"RIGHT JOIN {trackingName.Quoted()} [side] ");
            stringBuilder.Append("ON ");

            string empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();

                stringBuilder.Append($"{empty}[base].{columnName} = [side].{columnName}");
                stringBuilder1.Append($"{empty}[side].{columnName} = @{parameterName}");
                empty = " AND ";
            }

            stringBuilder.AppendLine();
            stringBuilder.Append(string.Concat("WHERE ", stringBuilder1.ToString()));

            sqlCommand.CommandText = stringBuilder.ToString();

            return sqlCommand;
        }



    }
}
