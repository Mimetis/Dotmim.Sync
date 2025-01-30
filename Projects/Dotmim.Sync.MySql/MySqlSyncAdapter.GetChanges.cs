using Dotmim.Sync.DatabaseStringParsers;
#if MARIADB
using Dotmim.Sync.MariaDB.Builders;
#elif MYSQL
using Dotmim.Sync.MySql.Builders;
#endif
using System.Linq;
using System.Text;

#if MARIADB
namespace Dotmim.Sync.MariaDB
#elif MYSQL
namespace Dotmim.Sync.MySql
#endif
{
    /// <summary>
    /// Represents a MySql Sync Adapter.
    /// </summary>
    public partial class MySqlSyncAdapter
    {
        //------------------------------------------------------------------
        // Select changes command
        //------------------------------------------------------------------

        /// <summary>
        /// Create all custom joins from within a filter.
        /// </summary>
        protected string CreateFilterCustomJoins(SyncFilter filter)
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

                var filterTableParser = new TableParser(filter.TableName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);

                var joinTableParser = new TableParser(customJoin.TableName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);

                var leftTableParser = new TableParser(customJoin.LeftTableName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);
                var leftTableName = leftTableParser.QuotedShortName;
                if (string.Equals(filterTableParser.QuotedShortName, leftTableName, SyncGlobalization.DataSourceStringComparison))
                    leftTableName = "`base`";

                var rightTableParser = new TableParser(customJoin.RightTableName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);
                var rightTableName = rightTableParser.QuotedShortName;
                if (string.Equals(filterTableParser.QuotedShortName, rightTableName, SyncGlobalization.DataSourceStringComparison))
                    rightTableName = "`base`";

                var leftColumnParser = new ObjectParser(customJoin.LeftColumnName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);
                var rightColumnParser = new ObjectParser(customJoin.RightColumnName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);

                stringBuilder.AppendLine($"{joinTableParser.QuotedShortName} ON {leftTableName}.{leftColumnParser.QuotedShortName} = {rightTableName}.{rightColumnParser.QuotedShortName}");
            }

            return stringBuilder.ToString();
        }

        /// <summary>
        /// Create all side where criteria from within a filter.
        /// </summary>
        protected string CreateFilterWhereSide(SyncFilter filter, bool checkTombstoneRows = false)
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

                var tableFilterParser = new TableParser(tableFilter.TableName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);
                var tableName = string.Equals(tableFilterParser.TableName, filter.TableName, SyncGlobalization.DataSourceStringComparison)
                    ? "`base`"
                    : tableFilterParser.QuotedShortName;

                var columnParser = new ObjectParser(columnFilter.ColumnName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);
                var parameterParser = new ObjectParser(whereFilter.ParameterName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);
                var param = filter.Parameters[parameterParser.NormalizedShortName];

                if (param == null)
                    throw new FilterParamColumnNotExistsException(columnParser.QuotedShortName, whereFilter.TableName);

                stringBuilder.Append($"{and2}({tableName}.{columnParser.QuotedShortName} = @{parameterParser.NormalizedShortName}");

                if (param.AllowNull)
                    stringBuilder.Append($" OR @{parameterParser.NormalizedShortName} IS NULL");

                stringBuilder.Append($")");

                and2 = " AND ";
            }

            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"  )");

            if (checkTombstoneRows)
            {
                stringBuilder.AppendLine($" OR `side`.`sync_row_is_tombstone` = 1");
                stringBuilder.AppendLine($")");
            }

            // Managing when state is tombstone
            return stringBuilder.ToString();
        }

        /// <summary>
        /// Create all custom wheres from witing a filter.
        /// </summary>
        protected string CreateFilterCustomWheres(SyncFilter filter)
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
                customWhereIteration = customWhereIteration.Replace("{{{", "`");
                customWhereIteration = customWhereIteration.Replace("}}}", "`");

                stringBuilder.Append($"{and2}{customWhereIteration}");
                and2 = " AND ";
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($")");

            return stringBuilder.ToString();
        }

        /// <summary>
        /// Return the correct command to get changes from the datasource.
        /// </summary>
        public string CreateSelectIncrementalChangesCommand(SyncFilter filter = null)
        {
            var stringBuilder = new StringBuilder(filter == null ? "SELECT" : "SELECT DISTINCT");

            // ----------------------------------
            // Add all columns
            // ----------------------------------
            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);

                var isPrimaryKey = this.TableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.AppendLine($"\t`side`.{columnParser.QuotedShortName}, ");
                else
                    stringBuilder.AppendLine($"\t`base`.{columnParser.QuotedShortName}, ");
            }

            stringBuilder.AppendLine($"\t`side`.`sync_row_is_tombstone`, ");
            stringBuilder.AppendLine($"\t`side`.`update_scope_id` as `sync_update_scope_id` ");
            stringBuilder.AppendLine($"FROM {this.MySqlObjectNames.TableQuotedShortName} `base`");

            // ----------------------------------
            // Make Right Join
            // ----------------------------------
            stringBuilder.Append($"RIGHT JOIN {this.MySqlObjectNames.TrackingTableQuotedShortName} `side` ON ");

            string empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.PrimaryKeys)
            {
                var columnParser = new ObjectParser(pkColumn, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);
                stringBuilder.Append($"{empty}`base`.{columnParser.QuotedShortName} = `side`.{columnParser.QuotedShortName}");
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
            stringBuilder.AppendLine("\t`side`.`timestamp` > @sync_min_timestamp");
            stringBuilder.AppendLine("\tAND (`side`.`update_scope_id` <> @sync_scope_id OR `side`.`update_scope_id` IS NULL) ");
            stringBuilder.AppendLine(");");

            return stringBuilder.ToString();
        }

        /// <summary>
        /// Returns a command text to get the initial changes.
        /// </summary>
        public string CreateSelectInitializedChangesCommand(SyncFilter filter = null)
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
                var columnParser = new ObjectParser(mutableColumn.ColumnName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);
                stringBuilder.AppendLine($"\t{comma}`base`.{columnParser.QuotedShortName}");
                comma = ", ";
            }

            stringBuilder.AppendLine($"\t, `side`.`sync_row_is_tombstone` as `sync_row_is_tombstone`");
            stringBuilder.AppendLine($"FROM {this.MySqlObjectNames.TableQuotedShortName} `base`");

            // ----------------------------------
            // Make Left Join
            // ----------------------------------
            stringBuilder.Append($"LEFT JOIN {this.MySqlObjectNames.TrackingTableQuotedShortName} `side` ON ");

            string empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.PrimaryKeys)
            {
                var columnParser = new ObjectParser(pkColumn, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);
                stringBuilder.Append($"{empty}`base`.{columnParser.QuotedShortName} = `side`.{columnParser.QuotedShortName}");
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
            stringBuilder.AppendLine("\t(`side`.`timestamp` > @sync_min_timestamp or @sync_min_timestamp IS NULL)");
            stringBuilder.AppendLine(")");
            stringBuilder.AppendLine("UNION");
            stringBuilder.AppendLine("SELECT");
            comma = "  ";
            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);

                var isPrimaryKey = this.TableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.AppendLine($"\t{comma}`side`.{columnParser.QuotedShortName}");
                else
                    stringBuilder.AppendLine($"\t{comma}`base`.{columnParser.QuotedShortName}");

                comma = ", ";
            }

            stringBuilder.AppendLine($"\t, `side`.`sync_row_is_tombstone` as `sync_row_is_tombstone`");
            stringBuilder.AppendLine($"FROM {this.MySqlObjectNames.TableQuotedShortName} `base`");

            // ----------------------------------
            // Make Left Join
            // ----------------------------------
            stringBuilder.Append($"RIGHT JOIN {this.MySqlObjectNames.TrackingTableQuotedShortName} `side` ON ");

            empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);
                stringBuilder.Append($"{empty}`base`.{columnParser.QuotedShortName} = `side`.{columnParser.QuotedShortName}");
                empty = " AND ";
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE (`side`.`timestamp` > @sync_min_timestamp AND `side`.`sync_row_is_tombstone` = 1);");

            return stringBuilder.ToString();
        }

        /// <summary>
        /// Returns a command text to select a row.
        /// </summary>
        public string CreateSelectRowCommand()
        {

            StringBuilder stringBuilder = new StringBuilder("SELECT ");
            stringBuilder.AppendLine();
            StringBuilder stringBuilder1 = new StringBuilder();
            string empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.PrimaryKeys)
            {
                var columnParser = new ObjectParser(pkColumn, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);
                stringBuilder1.Append($"{empty}`side`.{columnParser.QuotedShortName} = @{columnParser.NormalizedShortName}");
                empty = " AND ";
            }

            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);

                var isPrimaryKey = this.TableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.AppendLine($"\t`side`.{columnParser.QuotedShortName}, ");
                else
                    stringBuilder.AppendLine($"\t`base`.{columnParser.QuotedShortName}, ");
            }

            stringBuilder.AppendLine("\t`side`.`sync_row_is_tombstone`, ");
            stringBuilder.AppendLine("\t`side`.`update_scope_id` as `sync_update_scope_id`");
            stringBuilder.AppendLine($"FROM {this.MySqlObjectNames.TableQuotedShortName} `base`");
            stringBuilder.AppendLine($"RIGHT JOIN {this.MySqlObjectNames.TrackingTableQuotedShortName} `side` ON");

            string str = string.Empty;
            foreach (var pkColumn in this.TableDescription.PrimaryKeys)
            {
                var columnParser = new ObjectParser(pkColumn, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);
                stringBuilder.Append($"{str}`base`.{columnParser.QuotedShortName} = `side`.{columnParser.QuotedShortName}");
                str = " AND ";
            }

            stringBuilder.AppendLine();
            stringBuilder.Append("WHERE ");
            stringBuilder.Append(stringBuilder1);
            stringBuilder.Append(";");
            return stringBuilder.ToString();
        }
    }
}