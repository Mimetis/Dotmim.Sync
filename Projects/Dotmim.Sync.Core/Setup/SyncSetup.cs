using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    public class SyncSetup
    {
        public string ScopeName { get; set; }

        public SetupTables Tables { get; set; }

        public SetupFilters Filters { get; set; }

        /// <summary>
        /// Specify a prefix for naming stored procedure. Default is empty string
        /// </summary>
        public string StoredProceduresPrefix { get; set; }

        /// <summary>
        /// Specify a suffix for naming stored procedures. Default is empty string
        /// </summary>
        public string StoredProceduresSuffix { get; set; }

        /// <summary>
        /// Specify a prefix for naming stored procedure. Default is empty string
        /// </summary>
        public string TriggersPrefix { get; set; }

        /// <summary>
        /// Specify a suffix for naming stored procedures. Default is empty string
        /// </summary>
        public string TriggersSuffix { get; set; }

        /// <summary>
        /// Specify a prefix for naming tracking tables. Default is empty string
        /// </summary>
        public string TrackingTablesPrefix { get; set; }

        /// <summary>
        /// Specify a suffix for naming tracking tables.
        /// </summary>
        public string TrackingTablesSuffix { get; set; }

        /// <summary>
        /// Create a list of tables to be added to the sync process
        /// </summary>
        /// <param name="caseSensitive">Specify if table names are case sensitive. Default is false</param>
        public SyncSetup(IEnumerable<string> tables, string scopeName = SyncOptions.DefaultScopeName) : this()
        {
            this.ScopeName = scopeName;
            this.Tables.AddRange(tables);
        }

        public SyncSetup()
        {
            this.ScopeName = SyncOptions.DefaultScopeName;
            this.Tables = new SetupTables();
            this.Filters = new SetupFilters();
        }

    }
}
