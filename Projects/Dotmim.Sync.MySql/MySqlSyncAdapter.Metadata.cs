using Dotmim.Sync.DatabaseStringParsers;
#if MARIADB
using Dotmim.Sync.MariaDB.Builders;
#elif MYSQL
using Dotmim.Sync.MySql.Builders;
#endif
#if NET6_0 || NET8_0
using MySqlConnector;
#elif NETSTANDARD
using MySql.Data.MySqlClient;
#endif
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

        /// <summary>
        /// Return a command text to update local rows.
        /// </summary>
        public string CreateUpdateUntrackedRowsCommand()
        {
            var stringBuilder = new StringBuilder();
            var str1 = new StringBuilder();
            var str2 = new StringBuilder();
            var str3 = new StringBuilder();
            var str4 = MySqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.GetPrimaryKeysColumns(), "`side`", "`base`");

            stringBuilder.AppendLine($"INSERT INTO {this.MySqlObjectNames.TrackingTableQuotedShortName} (");

            var comma = string.Empty;
            foreach (var pkeyColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkeyColumn.ColumnName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);

                str1.Append($"{comma}{columnParser.QuotedShortName}");
                str2.Append($"{comma}`base`.{columnParser.QuotedShortName}");
                str3.Append($"{comma}`side`.{columnParser.QuotedShortName}");

                comma = ", ";
            }

            stringBuilder.Append(str1);
            stringBuilder.AppendLine($", `update_scope_id`, `sync_row_is_tombstone`, `timestamp`, `last_change_datetime`");
            stringBuilder.AppendLine($")");
            stringBuilder.Append($"SELECT ");
            stringBuilder.Append(str2);
            stringBuilder.AppendLine($", NULL, 0, {MySqlObjectNames.TimestampValue}, now()");
            stringBuilder.AppendLine($"FROM {this.MySqlObjectNames.TableQuotedShortName} as `base` WHERE NOT EXISTS");
            stringBuilder.Append($"(SELECT ");
            stringBuilder.Append(str3);
            stringBuilder.AppendLine($" FROM {this.MySqlObjectNames.TrackingTableQuotedShortName} as `side` ");
            stringBuilder.AppendLine($"WHERE {str4})");

            var r = stringBuilder.ToString();

            return r;
        }

        /// <summary>
        /// Returns a command text to fully reset a table.
        /// </summary>
        public string CreateResetCommand()
        {

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"DELETE FROM {this.MySqlObjectNames.TableQuotedShortName};");
            stringBuilder.AppendLine($"DELETE FROM {this.MySqlObjectNames.TrackingTableQuotedShortName};");
            stringBuilder.AppendLine();

            return stringBuilder.ToString();
        }
    }
}