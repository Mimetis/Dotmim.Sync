using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;

#if NET6_0 || NET8_0
using MySqlConnector;
#elif NETSTANDARD
using MySql.Data.MySqlClient;
#endif
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

#if MARIADB
namespace Dotmim.Sync.MariaDB.Builders
#elif MYSQL
namespace Dotmim.Sync.MySql.Builders
#endif
{
    /// <summary>
    /// My SQL Object Names.
    /// </summary>
    public partial class MySqlObjectNames
    {

        /// <summary>
        /// Gets the prefix parameter for MySql.
        /// </summary>
        public const string MYSQLPREFIXPARAMETER = "in_";

        /// <summary>
        /// Gets the timestamp value to use for a rowversion column.
        /// </summary>
        public const string TimestampValue = "ROUND(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(6)) * 10000)";

        /// <summary>
        /// Gets the left quote character.
        /// </summary>
        public const char LeftQuote = '`';

        /// <summary>
        /// Gets the right quote character.
        /// </summary>
        public const char RightQuote = '`';

        internal const string InsertTriggerName = "`{0}insert_trigger`";
        internal const string UpdateTriggerName = "`{0}update_trigger`";
        internal const string DeleteTriggerName = "`{0}delete_trigger`";

        internal const string SelectChangesProcName = "`{0}{1}changes`";
        internal const string SelectChangesProcNameWithFilters = "`{0}{1}{2}changes`";

        internal const string InitializeChangesProcName = "`{0}{1}initialize`";
        internal const string InitializeChangesProcNameWithFilters = "`{0}{1}{2}initialize`";

        internal const string SelectRowProcName = "`{0}{1}selectrow`";

        internal const string InsertProcName = "`{0}{1}insert`";
        internal const string UpdateProcName = "`{0}{1}update`";
        internal const string DeleteProcName = "`{0}{1}delete`";

        internal const string ResetProcName = "`{0}{1}reset`";

        internal const string InsertMetadataProcName = "`{0}{1}insertmetadata`";
        internal const string UpdateMetadataProcName = "`{0}{1}updatemetadata`";
        internal const string DeleteMetadataText = "DELETE FROM {0} WHERE `timestamp` <= @sync_row_timestamp;";

        internal const string DisableConstraintsText = "SET FOREIGN_KEY_CHECKS=0;";
        internal const string EnableConstraintsText = "SET FOREIGN_KEY_CHECKS=1;";

        /// <summary>
        /// Gets the table description.
        /// </summary>
        public SyncTable TableDescription { get; }

        /// <summary>
        /// Gets the scope info.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <summary>
        /// Gets the parsed tracking table name, wihtout any quotes characters.
        /// </summary>
        public string TrackingTableName { get; private set; }

        /// <summary>
        /// Gets the parsed normalized tracking table full name.
        /// </summary>
        public string TrackingTableNormalizedFullName { get; private set; }

        /// <summary>
        /// Gets the parsed normalized tracking table short name.
        /// </summary>
        public string TrackingTableNormalizedShortName { get; private set; }

        /// <summary>
        /// Gets the parsed quoted tracking table full name.
        /// </summary>
        public string TrackingTableQuotedFullName { get; private set; }

        /// <summary>
        /// Gets the parsed quoted tracking table short name.
        /// </summary>
        public string TrackingTableQuotedShortName { get; private set; }

        /// <summary>
        /// Gets the parsed tracking table schema name. if empty, "public" is returned.
        /// </summary>
        public string TrackingTableSchemaName { get; private set; }

        /// <summary>
        /// Gets the parsed table name, without any quotes characters.
        /// </summary>
        public string TableName { get; private set; }

        /// <summary>
        /// Gets the parsed normalized table full name (with schema, if any).
        /// </summary>
        public string TableNormalizedFullName { get; private set; }

        /// <summary>
        /// Gets the parsed normalized table short name (without schema, if any).
        /// </summary>
        public string TableNormalizedShortName { get; private set; }

        /// <summary>
        /// Gets the parsed quoted table full name (with schema, if any).
        /// </summary>
        public string TableQuotedFullName { get; private set; }

        /// <summary>
        /// Gets the parsed quoted table short name (without schema, if any).
        /// </summary>
        public string TableQuotedShortName { get; private set; }

        /// <summary>
        /// Gets the parsed table schema name. if empty, "public" is returned.
        /// </summary>
        public string TableSchemaName { get; private set; }

        /// <inheritdoc cref="MySqlObjectNames"/>
        public MySqlObjectNames(SyncTable tableDescription, ScopeInfo scopeInfo)
        {
            this.TableDescription = tableDescription;
            this.ScopeInfo = scopeInfo;

            //-------------------------------------------------
            // set table names
            var tableParser = new TableParser(this.TableDescription.GetFullName(), LeftQuote, RightQuote);

            this.TableName = tableParser.TableName;
            this.TableNormalizedFullName = tableParser.NormalizedFullName;
            this.TableNormalizedShortName = tableParser.NormalizedShortName;
            this.TableQuotedFullName = tableParser.QuotedFullName;
            this.TableQuotedShortName = tableParser.QuotedShortName;
            this.TableSchemaName = tableParser.SchemaName;

            //-------------------------------------------------
            // define tracking table name with prefix and suffix.
            // if no pref / suf, use default value
            var trakingTableNameString = string.IsNullOrEmpty(this.ScopeInfo.Setup?.TrackingTablesPrefix) && string.IsNullOrEmpty(this.ScopeInfo.Setup?.TrackingTablesSuffix)
                ? $"{this.TableDescription.TableName}_tracking"
                : $"{this.ScopeInfo.Setup?.TrackingTablesPrefix}{this.TableDescription.TableName}{this.ScopeInfo.Setup?.TrackingTablesSuffix}";

            if (!string.IsNullOrEmpty(this.TableDescription.SchemaName))
                trakingTableNameString = $"{this.TableDescription.SchemaName}.{trakingTableNameString}";

            // Parse
            var trackingTableParser = new TableParser(trakingTableNameString, LeftQuote, RightQuote);

            // set the tracking table names
            this.TrackingTableName = trackingTableParser.TableName;
            this.TrackingTableNormalizedFullName = trackingTableParser.NormalizedFullName;
            this.TrackingTableNormalizedShortName = trackingTableParser.NormalizedShortName;
            this.TrackingTableQuotedFullName = trackingTableParser.QuotedFullName;
            this.TrackingTableQuotedShortName = trackingTableParser.QuotedShortName;
            this.TrackingTableSchemaName = trackingTableParser.SchemaName;
        }

        /// <summary>
        /// Returns the stored procedure name for the given stored procedure type.
        /// </summary>
        public string GetStoredProcedureCommandName(DbStoredProcedureType storedProcedureType, SyncFilter filter = null)
        {
            var scopeNameWithoutDefaultScope = this.ScopeInfo.Name == SyncOptions.DefaultScopeName ? string.Empty : $"{this.ScopeInfo.Name}_";
            var storedProcedureNormalizedName = $"{this.ScopeInfo.Setup?.StoredProceduresPrefix}{this.TableNormalizedFullName}{this.ScopeInfo.Setup?.StoredProceduresSuffix}_";

            return storedProcedureType switch
            {
                DbStoredProcedureType.UpdateRow => string.Format(UpdateProcName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbStoredProcedureType.DeleteRow => string.Format(DeleteProcName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                _ => null,
            };
        }

        /// <summary>
        /// Returns the trigger name for the given trigger type.
        /// </summary>
        public string GetTriggerCommandName(DbTriggerType objectType, SyncFilter filter = null)
        {
            var triggerNormalizedName = $"{this.ScopeInfo.Setup?.TriggersPrefix}{this.TableNormalizedFullName}{this.ScopeInfo.Setup?.TriggersSuffix}_";

            return objectType switch
            {
                DbTriggerType.Update => string.Format(UpdateTriggerName, triggerNormalizedName),
                DbTriggerType.Insert => string.Format(InsertTriggerName, triggerNormalizedName),
                DbTriggerType.Delete => string.Format(DeleteTriggerName, triggerNormalizedName),
                _ => null,
            };
        }
    }
}