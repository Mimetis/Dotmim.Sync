using Dotmim.Sync.DatabaseStringParsers;
using Dotmim.Sync.PostgreSql.Builders;
using Npgsql;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.PostgreSql
{
    /// <summary>
    /// Npgsql sync adapter.
    /// </summary>
    public partial class NpgsqlSyncAdapter : DbSyncAdapter
    {

        // ---------------------------------------------------
        // Select Changes Command
        // ---------------------------------------------------

        /// <summary>
        /// Get the Select Changes Command.
        /// </summary>
        private (DbCommand Command, bool IsBatchCommand) GetSelectChangesCommand(SyncFilter filter = null)
        {
            StringBuilder stringBuilder = new StringBuilder();

            if (filter != null)
                stringBuilder.AppendLine("SELECT DISTINCT");
            else
                stringBuilder.AppendLine("SELECT");

            // ----------------------------------
            // Add all columns
            // ----------------------------------
            foreach (var pkColumn in this.TableDescription.PrimaryKeys)
            {
                var columnParser = new ObjectParser(pkColumn, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                stringBuilder.AppendLine($"\tside.{columnParser.QuotedShortName}, ");
            }

            foreach (var mutableColumn in this.TableDescription.GetMutableColumns())
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                stringBuilder.AppendLine($"\tbase.{columnParser.QuotedShortName}, ");
            }

            stringBuilder.AppendLine($"\tside.\"sync_row_is_tombstone\", ");
            stringBuilder.AppendLine($"\tside.\"update_scope_id\" as \"sync_update_scope_id\" ");

            // ----------------------------------
            stringBuilder.AppendLine($"FROM {this.NpgsqlObjectNames.TableQuotedFullName} base");

            // ----------------------------------
            // Make Right Join
            // ----------------------------------
            stringBuilder.Append($"RIGHT JOIN {this.NpgsqlObjectNames.TrackingTableQuotedFullName} side ON ");

            string empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.PrimaryKeys)
            {
                var columnParser = new ObjectParser(pkColumn, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                stringBuilder.Append($"{empty}base.{columnParser.QuotedShortName} = side.{columnParser.QuotedShortName}");
                empty = " AND ";
            }

            // ----------------------------------
            // Custom Joins
            // ----------------------------------
            if (filter != null)
                stringBuilder.Append(this.CreateFilterCustomJoins(filter));

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE (");

            // ----------------------------------
            // Where filters and Custom Where string
            // ----------------------------------
            if (filter != null)
            {
                var createFilterWhereSide = this.CreateFilterWhereSide(filter, true);
                stringBuilder.Append(createFilterWhereSide);

                if (!string.IsNullOrEmpty(createFilterWhereSide))
                    stringBuilder.AppendLine($"AND ");

                var createFilterCustomWheres = this.CreateFilterCustomWheres(filter);
                stringBuilder.Append(createFilterCustomWheres);

                if (!string.IsNullOrEmpty(createFilterCustomWheres))
                    stringBuilder.AppendLine($"AND ");
            }

            // ----------------------------------
            stringBuilder.AppendLine("\tside.\"timestamp\" > @sync_min_timestamp");
            stringBuilder.AppendLine("\tAND (side.\"update_scope_id\" <> @sync_scope_id OR side.\"update_scope_id\" IS NULL)");
            stringBuilder.AppendLine(");");

            var sqlCommand = new NpgsqlCommand
            {
                CommandType = CommandType.Text,
                CommandText = stringBuilder.ToString(),
            };

            return (sqlCommand, false);
        }

        // ---------------------------------------------------
        // Select Initialize Changes Command
        // ---------------------------------------------------
        private (DbCommand Command, bool IsBatchCommand) GetSelectInitializedChangesCommand(SyncFilter filter = null)
        {
            var stringBuilder = new StringBuilder();

            // if we have a filter we may have joins that will duplicate lines
            if (filter != null)
                stringBuilder.AppendLine("SELECT DISTINCT");
            else
                stringBuilder.AppendLine("SELECT");

            var comma = "  ";
            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnPaser = new ObjectParser(mutableColumn.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                stringBuilder.AppendLine($"\t{comma}base.{columnPaser.QuotedShortName}");
                comma = ", ";
            }

            stringBuilder.AppendLine($"\t, side.\"sync_row_is_tombstone\" as \"sync_row_is_tombstone\"");
            stringBuilder.AppendLine($"FROM {this.NpgsqlObjectNames.TableQuotedFullName} base");

            // ----------------------------------
            // Make Left Join
            // ----------------------------------
            stringBuilder.Append($"LEFT JOIN {this.NpgsqlObjectNames.TrackingTableQuotedFullName} side ON ");

            string empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnPaser = new ObjectParser(pkColumn.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                stringBuilder.Append($"{empty}base.{columnPaser.QuotedShortName} = side.{columnPaser.QuotedShortName}");
                empty = " AND ";
            }

            // ----------------------------------
            // Custom Joins
            // ----------------------------------
            if (filter != null)
                stringBuilder.Append(this.CreateFilterCustomJoins(filter));

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE (");

            // ----------------------------------
            // Where filters and Custom Where string
            // ----------------------------------
            if (filter != null)
            {
                var createFilterWhereSide = this.CreateFilterWhereSide(filter);
                stringBuilder.Append(createFilterWhereSide);

                if (!string.IsNullOrEmpty(createFilterWhereSide))
                    stringBuilder.AppendLine($"AND ");

                var createFilterCustomWheres = this.CreateFilterCustomWheres(filter);
                stringBuilder.Append(createFilterCustomWheres);

                if (!string.IsNullOrEmpty(createFilterCustomWheres))
                    stringBuilder.AppendLine($"AND ");
            }

            // ----------------------------------
            stringBuilder.AppendLine("\t(side.\"timestamp\" > @sync_min_timestamp OR  @sync_min_timestamp IS NULL)");
            stringBuilder.AppendLine(")");
            stringBuilder.AppendLine("UNION");
            stringBuilder.AppendLine("SELECT");
            comma = "  ";
            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                var isPrimaryKey = this.TableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.AppendLine($"\t{comma}side.{columnParser.QuotedShortName}");
                else
                    stringBuilder.AppendLine($"\t{comma}base.{columnParser.QuotedShortName}");

                comma = ", ";
            }

            stringBuilder.AppendLine($"\t, side.\"sync_row_is_tombstone\" as \"sync_row_is_tombstone\"");
            stringBuilder.AppendLine($"FROM {this.NpgsqlObjectNames.TableQuotedFullName} base");

            // ----------------------------------
            // Make Left Join
            // ----------------------------------
            stringBuilder.Append($"RIGHT JOIN {this.NpgsqlObjectNames.TrackingTableQuotedFullName} side ON ");

            empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                stringBuilder.Append($"{empty}base.{columnParser.QuotedShortName} = side.{columnParser.QuotedShortName}");
                empty = " AND ";
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE (side.\"timestamp\" > @sync_min_timestamp AND \"side\".\"sync_row_is_tombstone\" = 1);");

            var sqlCommand = new NpgsqlCommand
            {
                CommandType = CommandType.Text,
                CommandText = stringBuilder.ToString(),
            };

            return (sqlCommand, false);
        }

        //----------------------------------------------------
        private string CreateFilterCustomJoins(SyncFilter filter)
        {
            var customJoins = filter.Joins;

            if (customJoins.Count == 0)
                return string.Empty;

            var stringBuilder = new StringBuilder();

            stringBuilder.AppendLine();
            foreach (var customJoin in customJoins)
            {
                switch (customJoin.JoinEnum)
                {
                    case Join.Left:
                        stringBuilder.Append("LEFT JOIN ");
                        break;
                    case Join.Right:
                        stringBuilder.Append("RIGHT JOIN ");
                        break;
                    case Join.Outer:
                        stringBuilder.Append("OUTER JOIN ");
                        break;
                    case Join.Inner:
                    default:
                        stringBuilder.Append("INNER JOIN ");
                        break;
                }

                var fullTableName = string.IsNullOrEmpty(filter.SchemaName) ? filter.TableName : $"{filter.SchemaName}.{filter.TableName}";
                var filterTableParser = new TableParser(fullTableName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                var filterTableName = filterTableParser.QuotedFullName;

                var fullJoinTableName = string.IsNullOrEmpty(customJoin.TableSchemaName) ? customJoin.TableName : $"{customJoin.TableSchemaName}.{customJoin.TableName}";
                var joinTableParser = new TableParser(fullJoinTableName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                var joinTableName = joinTableParser.QuotedFullName;

                var fullLeftTableName = string.IsNullOrEmpty(customJoin.LeftTableSchemaName) ? customJoin.LeftTableName : $"{customJoin.LeftTableSchemaName}.{customJoin.LeftTableName}";
                var leftTableParser = new TableParser(fullLeftTableName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                var leftTableName = leftTableParser.QuotedFullName;
                if (string.Equals(filterTableName, leftTableName, SyncGlobalization.DataSourceStringComparison))
                    leftTableName = "base";

                var fullRightTableName = string.IsNullOrEmpty(customJoin.RightTableSchemaName) ? customJoin.RightTableName : $"{customJoin.RightTableSchemaName}.{customJoin.RightTableName}";
                var rightTableParser = new TableParser(fullRightTableName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                var rightTableName = rightTableParser.QuotedFullName;
                if (string.Equals(filterTableName, rightTableName, SyncGlobalization.DataSourceStringComparison))
                    rightTableName = "base";

                var leftColumnParser = new ObjectParser(customJoin.LeftColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                var leftColumName = leftColumnParser.QuotedShortName;
                var rightColumnParser = new ObjectParser(customJoin.RightColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                var rightColumName = rightColumnParser.QuotedShortName;

                stringBuilder.AppendLine($"{joinTableName} ON {leftTableName}.{leftColumName} = {rightTableName}.{rightColumName}");
            }

            return stringBuilder.ToString();
        }

        private string CreateFilterCustomWheres(SyncFilter filter)
        {
            var customWheres = filter.CustomWheres;

            if (customWheres.Count == 0)
                return string.Empty;

            var stringBuilder = new StringBuilder();
            var and2 = "  ";
            stringBuilder.AppendLine($"(");

            foreach (var customWhere in customWheres)
            {
                // Template escape character
                var customWhereIteration = customWhere;
                customWhereIteration = customWhereIteration.Replace("{{{", "\"", SyncGlobalization.DataSourceStringComparison);
                customWhereIteration = customWhereIteration.Replace("}}}", "\"", SyncGlobalization.DataSourceStringComparison);

                stringBuilder.Append($"{and2}{customWhereIteration}");
                and2 = " AND ";
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($")");

            return stringBuilder.ToString();
        }

        private string CreateFilterWhereSide(SyncFilter filter, bool checkTombstoneRows = false)
        {
            var sideWhereFilters = filter.Wheres;

            if (sideWhereFilters.Count == 0)
                return string.Empty;

            var stringBuilder = new StringBuilder();

            // Managing when state is tombstone
            if (checkTombstoneRows)
                stringBuilder.AppendLine($"(");

            stringBuilder.AppendLine($" (");

            var and2 = "   ";

            foreach (var whereFilter in sideWhereFilters)
            {
                var tableFilter = this.TableDescription.Schema.Tables[whereFilter.TableName, whereFilter.SchemaName];
                if (tableFilter == null)
                    throw new FilterParamTableNotExistsException(whereFilter.TableName);

                var columnFilter = tableFilter.Columns[whereFilter.ColumnName];
                if (columnFilter == null)
                    throw new FilterParamColumnNotExistsException(whereFilter.ColumnName, whereFilter.TableName);

                var tableParser = new TableParser(tableFilter.GetFullName(), NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                var tableName = tableParser.TableName;
                tableName = string.Equals(tableName, filter.TableName, SyncGlobalization.DataSourceStringComparison)
                    ? "\"base\""
                    : tableParser.QuotedFullName;

                var columnParser = new ObjectParser(columnFilter.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                var columnName = columnParser.QuotedShortName;
                var paramParser = new ObjectParser(whereFilter.ParameterName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                var parameterName = paramParser.NormalizedShortName;

                var param = filter.Parameters[parameterName];

                if (param == null)
                    throw new FilterParamColumnNotExistsException(columnName, whereFilter.TableName);

                stringBuilder.Append($"{and2}({tableName}.{columnName} = @{parameterName}");

                if (param.AllowNull)
                    stringBuilder.Append($" OR @{parameterName} IS NULL");

                stringBuilder.Append($")");

                and2 = " AND ";
            }

            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"  )");

            if (checkTombstoneRows)
            {
                stringBuilder.AppendLine($" OR side.sync_row_is_tombstone = 1");
                stringBuilder.AppendLine($")");
            }

            // Managing when state is tombstone
            return stringBuilder.ToString();
        }
    }
}