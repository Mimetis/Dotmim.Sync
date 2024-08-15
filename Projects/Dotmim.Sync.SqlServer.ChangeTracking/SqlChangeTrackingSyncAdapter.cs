using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;
using Dotmim.Sync.SqlServer.Builders;
using Dotmim.Sync.SqlServer.ChangeTracking.Builders;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.SqlServer
{
    /// <inheritdoc />
    public class SqlChangeTrackingSyncAdapter : SqlSyncAdapter
    {

        /// <inheritdoc />
        public SqlChangeTrackingSyncAdapter(SyncTable tableDescription, ScopeInfo scopeInfo, bool useBulkOperations)
            : base(tableDescription, scopeInfo, useBulkOperations)
        {
        }

        /// <inheritdoc />
        public override DbTableBuilder GetTableBuilder() => new SqlChangeTrackingTableBuilder(this.TableDescription, this.ScopeInfo);

        /// <summary>
        /// Overriding adapter since the update metadata is not a stored proc that we can override.
        /// </summary>
        public override (DbCommand, bool) GetCommand(SyncContext context, DbCommandType commandType, SyncFilter filter)
        {
            return commandType switch
            {
                DbCommandType.UpdateMetadata => (this.BuildUpdateMetadataCommand(), false),
                DbCommandType.SelectRow => (this.BuildSelectRowCommand(), false),
                DbCommandType.DeleteMetadata => (null, false),
                DbCommandType.Reset => (this.CreateResetCommand(), false),
                DbCommandType.UpdateUntrackedRows => (this.BuildUpdateUntrackedRowsCommand(), false),
                _ => base.GetCommand(context, commandType, filter),
            };
        }

        private SqlCommand BuildUpdateMetadataCommand()
        {
            var c = new SqlCommand("Set @sync_row_count = 1;");
            c.Parameters.Add("@sync_row_count", SqlDbType.Int);
            return c;
        }

        private SqlCommand BuildSelectRowCommand()
        {
            var sqlCommand = new SqlCommand();
            var stringBuilder1 = new StringBuilder();

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine(";WITH ");
            stringBuilder.AppendLine($"  {this.SqlObjectNames.TrackingTableQuotedShortName} AS (");
            stringBuilder.Append("\tSELECT ");
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
                stringBuilder.Append($"[CT].{columnParser.QuotedShortName} as {columnParser.QuotedShortName}, ");
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("\tCAST([CT].[SYS_CHANGE_CONTEXT] as uniqueidentifier) AS [sync_update_scope_id], ");
            stringBuilder.AppendLine("\t[CT].[SYS_CHANGE_VERSION] as [sync_timestamp],");
            stringBuilder.AppendLine("\tCASE WHEN [CT].[SYS_CHANGE_OPERATION] = 'D' THEN 1 ELSE 0 END AS [sync_row_is_tombstone]");
            stringBuilder.AppendLine($"\tFROM CHANGETABLE(CHANGES {this.SqlObjectNames.TableQuotedFullName}, NULL) AS [CT]");
            stringBuilder.AppendLine("\t)");

            stringBuilder.AppendLine("SELECT ");

            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
                var isPrimaryKey = this.TableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.AppendLine($"\t[side].{columnParser.QuotedShortName}, ");
                else
                    stringBuilder.AppendLine($"\t[base].{columnParser.QuotedShortName}, ");
            }

            stringBuilder.AppendLine($"\t[side].[sync_row_is_tombstone] as [sync_row_is_tombstone], ");
            stringBuilder.AppendLine($"\t[side].[sync_update_scope_id] as [sync_update_scope_id] ");
            stringBuilder.AppendLine($"FROM {this.SqlObjectNames.TableQuotedFullName} [base]");
            stringBuilder.Append($"RIGHT JOIN {this.SqlObjectNames.TrackingTableQuotedShortName} [side] ");
            stringBuilder.Append("ON ");

            string empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);

                stringBuilder.Append($"{empty}[base].{columnParser.QuotedShortName} = [side].{columnParser.QuotedShortName}");
                stringBuilder1.Append($"{empty}[side].{columnParser.QuotedShortName} = @{columnParser.NormalizedShortName}");
                empty = " AND ";
            }

            stringBuilder.AppendLine();
            stringBuilder.Append(string.Concat("WHERE ", stringBuilder1.ToString()));

            sqlCommand.CommandText = stringBuilder.ToString();

            return sqlCommand;
        }

        private SqlCommand CreateResetCommand()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"SET @sync_row_count = 0;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"DELETE FROM {this.SqlObjectNames.TableQuotedFullName};");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("SET @sync_row_count = @@ROWCOUNT;"));

            return new SqlCommand(stringBuilder.ToString());
        }

        private SqlCommand BuildUpdateUntrackedRowsCommand()
        {

            var mutablesColumns = this.TableDescription.GetMutableColumns(false, false).ToList();

            if (mutablesColumns.Count <= 0)
                return null;

            // Get a mutable column
            var mutableColumn = mutablesColumns[0];
            var mutableColumnParser = new ObjectParser(mutableColumn.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);

            var stringBuilder = new StringBuilder();
            string comma = string.Empty;
            stringBuilder.AppendLine($";WITH [side] AS (");
            stringBuilder.AppendLine($"SELECT ");
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
                stringBuilder.AppendLine($"\t{comma}[base].{columnParser.QuotedShortName} AS {columnParser.QuotedShortName} ");
                comma = ",";
            }

            stringBuilder.AppendLine($"FROM {this.SqlObjectNames.TableQuotedFullName} as [base]");
            stringBuilder.AppendLine($"LEFT JOIN CHANGETABLE(CHANGES {this.SqlObjectNames.TableQuotedFullName}, NULL) as CT ON ");
            comma = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
                stringBuilder.AppendLine($"\t{comma} [base].{columnParser.QuotedShortName} = [CT].{columnParser.QuotedShortName} ");
                comma = "AND";
            }

            stringBuilder.AppendLine($"WHERE (");
            comma = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
                stringBuilder.AppendLine($"\t{comma} [CT].{columnParser.QuotedShortName} IS NULL ");
                comma = "AND";
            }

            stringBuilder.AppendLine($") OR (CT.[SYS_CHANGE_VERSION] < (SELECT min(scope_last_sync_timestamp) from dbo.scope_info_client)))");
            stringBuilder.AppendLine($"UPDATE [base] SET [base].{mutableColumnParser.QuotedShortName} = [base].{mutableColumnParser.QuotedShortName}");
            stringBuilder.AppendLine($"FROM {this.SqlObjectNames.TableQuotedFullName} as [base]");
            stringBuilder.AppendLine($"JOIN [side] ON");
            comma = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
                stringBuilder.AppendLine($"\t{comma} [base].{columnParser.QuotedShortName} = [side].{columnParser.QuotedShortName} ");
                comma = "AND";
            }

            var r = stringBuilder.ToString();

            return new SqlCommand(stringBuilder.ToString());
        }
    }
}