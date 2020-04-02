using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    [DataContract(Name = "s"), Serializable]
    public class SyncSetup
    {

        /// <summary>
        /// Gets or Sets the tables involved in the sync
        /// </summary>
        [DataMember(Name = "tbls", IsRequired = false, EmitDefaultValue = false, Order = 1)]
        public SetupTables Tables { get; set; }

        /// <summary>
        /// Gets or Sets the filters involved in the sync
        /// </summary>
        [DataMember(Name = "fils", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public SetupFilters Filters { get; set; }

        /// <summary>
        /// Specify a prefix for naming stored procedure. Default is empty string
        /// </summary>
        [DataMember(Name = "spp", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public string StoredProceduresPrefix { get; set; }

        /// <summary>
        /// Specify a suffix for naming stored procedures. Default is empty string
        /// </summary>
        [DataMember(Name = "sps", IsRequired = false, EmitDefaultValue = false, Order = 4)]
        public string StoredProceduresSuffix { get; set; }

        /// <summary>
        /// Specify a prefix for naming stored procedure. Default is empty string
        /// </summary>
        [DataMember(Name = "tf", IsRequired = false, EmitDefaultValue = false, Order = 5)]
        public string TriggersPrefix { get; set; }

        /// <summary>
        /// Specify a suffix for naming stored procedures. Default is empty string
        /// </summary>
        [DataMember(Name = "ts", IsRequired = false, EmitDefaultValue = false, Order = 6)]
        public string TriggersSuffix { get; set; }

        /// <summary>
        /// Specify a prefix for naming tracking tables. Default is empty string
        /// </summary>
        [DataMember(Name = "ttp", IsRequired = false, EmitDefaultValue = false, Order = 7)]
        public string TrackingTablesPrefix { get; set; }

        /// <summary>
        /// Specify a suffix for naming tracking tables.
        /// </summary>
        [DataMember(Name = "tts", IsRequired = false, EmitDefaultValue = false, Order = 8)]
        public string TrackingTablesSuffix { get; set; }

        /// <summary>
        /// Gets or Sets the current Setup version.
        /// </summary>
        [DataMember(Name = "v", IsRequired = false, EmitDefaultValue = false, Order = 9)]
        public string Version { get; set; }

        /// <summary>
        /// Create a list of tables to be added to the sync process
        /// </summary>
        /// <param name="caseSensitive">Specify if table names are case sensitive. Default is false</param>
        public SyncSetup(IEnumerable<string> tables) : this() => this.Tables.AddRange(tables);

        /// <summary>
        /// ctor
        /// </summary>
        public SyncSetup()
        {
            this.Tables = new SetupTables();
            this.Filters = new SetupFilters();
            this.Version = "1";
        }

        /// <summary>
        /// Check if Setup has tables
        /// </summary>
        public bool HasTables => this.Tables?.Count > 0;

        /// <summary>
        /// Check if Setup has at least one table with columns
        /// </summary>
        public bool HasColumns => this.Tables?.SelectMany(t => t.Columns).Count() > 0;  // using SelectMany to get columns and not Collection<Column>


    }
}
