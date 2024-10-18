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
        public override (DbCommand, bool) GetCommand(SyncContext context, DbCommandType commandType, SyncFilter filter) => commandType switch
        {
            DbCommandType.UpdateMetadata => (this.BuildUpdateMetadataCommand(), false),
            DbCommandType.SelectRow => (this.BuildSelectRowCommand(), false),
            DbCommandType.DeleteMetadata => (this.BuildDeleteMetadataCommand(), false),
            DbCommandType.Reset => (this.CreateResetCommand(), false),
            DbCommandType.UpdateUntrackedRows => (this.BuildUpdateUntrackedRowsCommand(), false),
            DbCommandType.None => throw new System.NotImplementedException(),
            DbCommandType.SelectChanges => throw new System.NotImplementedException(),
            DbCommandType.SelectInitializedChanges => throw new System.NotImplementedException(),
            DbCommandType.SelectInitializedChangesWithFilters => throw new System.NotImplementedException(),
            DbCommandType.SelectChangesWithFilters => throw new System.NotImplementedException(),
            DbCommandType.UpdateRow => throw new System.NotImplementedException(),
            DbCommandType.InsertRow => throw new System.NotImplementedException(),
            DbCommandType.DeleteRow => throw new System.NotImplementedException(),
            DbCommandType.DisableConstraints => throw new System.NotImplementedException(),
            DbCommandType.EnableConstraints => throw new System.NotImplementedException(),
            DbCommandType.SelectMetadata => throw new System.NotImplementedException(),
            DbCommandType.InsertTrigger => throw new System.NotImplementedException(),
            DbCommandType.UpdateTrigger => throw new System.NotImplementedException(),
            DbCommandType.DeleteTrigger => throw new System.NotImplementedException(),
            DbCommandType.UpdateRows => throw new System.NotImplementedException(),
            DbCommandType.InsertRows => throw new System.NotImplementedException(),
            DbCommandType.DeleteRows => throw new System.NotImplementedException(),
            DbCommandType.BulkTableType => throw new System.NotImplementedException(),
            DbCommandType.PreUpdateRows => throw new System.NotImplementedException(),
            DbCommandType.PreInsertRows => throw new System.NotImplementedException(),
            DbCommandType.PreDeleteRows => throw new System.NotImplementedException(),
            DbCommandType.PreUpdateRow => throw new System.NotImplementedException(),
            DbCommandType.PreInsertRow => throw new System.NotImplementedException(),
            DbCommandType.PreDeleteRow => throw new System.NotImplementedException(),
            _ => base.GetCommand(context, commandType, filter),
        };

        private SqlCommand BuildUpdateMetadataCommand()
        {
            var c = new SqlCommand("Set @sync_row_count = 1;");
            c.Parameters.Add("@sync_row_count", SqlDbType.Int);
            return c;
        }

        private SqlCommand BuildDeleteMetadataCommand()
        {
            return null;

            // a lot of users are experiencing issues with this command, with a downgrade of the performances.
            // So, we are not using it anymore

            // var sqlCommand = new SqlCommand();
            // sqlCommand.CommandText = $"EXEC sys.sp_flush_CT_internal_table_on_demand;";
            // return sqlCommand;
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
            stringBuilder.AppendLine($"DELETE FROM {this.SqlObjectNames.TableQuotedFullName};");
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

            return new SqlCommand(stringBuilder.ToString());
        }
    }
}