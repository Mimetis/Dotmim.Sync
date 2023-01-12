using Npgsql;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using System.Text;
using Dotmim.Sync.Builders;
using NpgsqlTypes;
using System.Linq;

namespace Dotmim.Sync.PostgreSql
{
    public partial class NpgsqlSyncAdapter : DbSyncAdapter
    {

        // ---------------------------------------------------
        // Select Changes Command
        // ---------------------------------------------------

        /// <summary>
        /// Get the Select Changes Command
        /// </summary>
        private (DbCommand, bool) GetSelectChangesCommand(SyncFilter filter = null)
        {
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(TableName);

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
                var columnName = ParserName.Parse(pkColumn, "\"").Quoted().ToString();
                stringBuilder.AppendLine($"\tside.{columnName}, ");
            }
            foreach (var mutableColumn in this.TableDescription.GetMutableColumns())
            {
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                stringBuilder.AppendLine($"\tbase.{columnName}, ");
            }
            stringBuilder.AppendLine($"\tside.\"sync_row_is_tombstone\", ");
            stringBuilder.AppendLine($"\tside.\"update_scope_id\" as \"sync_update_scope_id\" ");
            // ----------------------------------
            stringBuilder.AppendLine($"FROM \"{schema}\".{TableName.Quoted()} base");

            // ----------------------------------
            // Make Right Join
            // ----------------------------------
            stringBuilder.Append($"RIGHT JOIN \"{schema}\".{TrackingTableName.Quoted()} side ON ");

            string empty = "";
            foreach (var pkColumn in this.TableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn, "\"").Quoted().ToString();
                stringBuilder.Append($"{empty}base.{columnName} = side.{columnName}");
                empty = " AND ";
            }

            // ----------------------------------
            // Custom Joins
            // ----------------------------------
            if (filter != null)
                stringBuilder.Append(CreateFilterCustomJoins(filter));

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE (");

            // ----------------------------------
            // Where filters and Custom Where string
            // ----------------------------------
            if (filter != null)
            {
                var createFilterWhereSide = CreateFilterWhereSide(filter, true);
                stringBuilder.Append(createFilterWhereSide);

                if (!string.IsNullOrEmpty(createFilterWhereSide))
                    stringBuilder.AppendLine($"AND ");

                var createFilterCustomWheres = CreateFilterCustomWheres(filter);
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
                CommandText = stringBuilder.ToString()
            };

            return (sqlCommand, false);
        }

        // ---------------------------------------------------
        // Select Initialize Changes Command
        // ---------------------------------------------------

        private (DbCommand, bool) GetSelectInitializedChangesCommand(SyncFilter filter = null)
        {
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(TableName);
            var stringBuilder = new StringBuilder();

            // if we have a filter we may have joins that will duplicate lines
            if (filter != null)
                stringBuilder.AppendLine("SELECT DISTINCT");
            else
                stringBuilder.AppendLine("SELECT");

            var comma = "  ";
            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                stringBuilder.AppendLine($"\t{comma}base.{ParserName.Parse(mutableColumn, "\"").Quoted()}");
                comma = ", ";
            }
            stringBuilder.AppendLine($"\t, side.\"sync_row_is_tombstone\" as \"sync_row_is_tombstone\"");
            stringBuilder.AppendLine($"FROM \"{schema}\".{TableName.Quoted()} base");

            // ----------------------------------
            // Make Left Join
            // ----------------------------------
            stringBuilder.Append($"LEFT JOIN \"{schema}\".{TrackingTableName.Quoted()} side ON ");

            string empty = "";
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn, "\"").Quoted().ToString();
                stringBuilder.Append($"{empty}base.{columnName} = side.{columnName}");
                empty = " AND ";
            }

            // ----------------------------------
            // Custom Joins
            // ----------------------------------
            if (filter != null)
                stringBuilder.Append(CreateFilterCustomJoins(filter));

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE (");

            // ----------------------------------
            // Where filters and Custom Where string
            // ----------------------------------
            if (filter != null)
            {
                var createFilterWhereSide = CreateFilterWhereSide(filter);
                stringBuilder.Append(createFilterWhereSide);

                if (!string.IsNullOrEmpty(createFilterWhereSide))
                    stringBuilder.AppendLine($"AND ");

                var createFilterCustomWheres = CreateFilterCustomWheres(filter);
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
                var columnName = ParserName.Parse(mutableColumn, "\"").Quoted().ToString();
                var isPrimaryKey = this.TableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.AppendLine($"\t{comma}side.{columnName}");
                else
                    stringBuilder.AppendLine($"\t{comma}base.{columnName}");

                comma = ", ";
            }
            stringBuilder.AppendLine($"\t, side.\"sync_row_is_tombstone\" as \"sync_row_is_tombstone\"");
            stringBuilder.AppendLine($"FROM \"{schema}\".{TableName.Quoted()} base");

            // ----------------------------------
            // Make Left Join
            // ----------------------------------
            stringBuilder.Append($"RIGHT JOIN \"{schema}\".{TrackingTableName.Quoted()} side ON ");

            empty = "";
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn, "\"").Quoted().ToString();
                stringBuilder.Append($"{empty}base.{columnName} = side.{columnName}");
                empty = " AND ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE (side.\"timestamp\" > @sync_min_timestamp AND \"side\".\"sync_row_is_tombstone\" = True);");
            
            var sqlCommand = new NpgsqlCommand
            {
                CommandType = CommandType.Text,
                CommandText = stringBuilder.ToString()
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
                var filterTableName = ParserName.Parse(fullTableName, "\"").Quoted().Schema().ToString();

                var joinTableName = ParserName.Parse(customJoin.TableName, "\"").Quoted().Schema().ToString();

                var leftTableName = ParserName.Parse(customJoin.LeftTableName, "\"").Quoted().Schema().ToString();
                if (string.Equals(filterTableName, leftTableName, SyncGlobalization.DataSourceStringComparison))
                    leftTableName = "base";

                var rightTableName = ParserName.Parse(customJoin.RightTableName, "\"").Quoted().Schema().ToString();
                if (string.Equals(filterTableName, rightTableName, SyncGlobalization.DataSourceStringComparison))
                    rightTableName = "base";

                var leftColumName = ParserName.Parse(customJoin.LeftColumnName, "\"").Quoted().ToString();
                var rightColumName = ParserName.Parse(customJoin.RightColumnName, "\"").Quoted().ToString();

                stringBuilder.AppendLine($"{joinTableName} ON {leftTableName}.{leftColumName} = {rightTableName}.{rightColumName}");
            }

            return stringBuilder.ToString();
        }

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
                // IF coming from Mysql
                var customWhereIteration = customWhere.Replace("`", "\"");

                // If coming from SQL
                customWhereIteration = customWhereIteration.Replace("[", "\"");
                customWhereIteration = customWhereIteration.Replace("]", "\"");

                stringBuilder.Append($"{and2}{customWhereIteration}");
                and2 = " AND ";
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($")");

            return stringBuilder.ToString();
        }

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

                var tableName = ParserName.Parse(tableFilter, "\"").Unquoted().ToString();
                if (string.Equals(tableName, filter.TableName, SyncGlobalization.DataSourceStringComparison))
                    tableName = "\"base\"";
                else
                    tableName = ParserName.Parse(tableFilter, "\"").Quoted().ToString();

                var columnName = ParserName.Parse(columnFilter, "\"").Quoted().ToString();
                var parameterName = ParserName.Parse(whereFilter.ParameterName, "\"").Unquoted().Normalized().ToString();

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
                stringBuilder.AppendLine($" OR side.sync_row_is_tombstone = TRUE");
                stringBuilder.AppendLine($")");
            }
            // Managing when state is tombstone

            return stringBuilder.ToString();
        }

    }
}
